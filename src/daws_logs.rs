use anyhow::{Context, Result, anyhow};
use aws_config::BehaviorVersion;
use aws_sdk_cloudwatchlogs::types::LogStream;
use aws_sdk_cloudwatchlogs::{Client as logs_client, types::OrderBy as logs_OrderBy};
use aws_sdk_glue::Client as glue_client;
use aws_sdk_sts::Client as sts_client;
use clap::{ArgGroup, Parser};
use futures::StreamExt;
// streamext to map iter futures
// use std::error::Error; // trait to do  error.source();
use regex::Regex;
use std::future::Future;
use std::time::Instant;
// use std::io::Write; // needed to flush

static CONCURRENCY_LIMIT: usize = 500;
static IDLE_LIMIT: u64 = 600;
use std::collections::HashMap;

use crate::printcr;

#[derive(Clone)]
struct StreamState {
    next_token: Option<String>, // String cannot derive Copy!
}
// An alias!!!! Wow
type StreamTracker = HashMap<String, StreamState>;
enum Service {
    Generic, // variants
    Glue,
    Lambda,
}

#[derive(Parser)]
#[command(group(ArgGroup::new("Main").args(&["stream","last_n_hours"])))]
struct Args {
    #[arg(short, long, default_value = "djm_dev")]
    profile: String,
    #[arg(short, long)]
    group: String,
    #[arg(short, long, help = "This is optional for lambda logs")]
    stream: Option<String>,
    #[arg(
        short,
        long,
        default_value_t = 1_u64,
        help = "Use when getting lambda logs"
    )]
    last_n_hours: u64,
}
/*
enum GlueJobRunStatus {
    Starting,
    Running,
    Stopping,
    Stopped,
    Succeeded,
    Failed,
    Timeout,
    Na,
}
*/

struct Client {
    sts: sts_client,
    logs: logs_client,
    glue: glue_client,
}

// #[derive(copy,clone)] // before implement this, check that your methods have &self, if want to modify field then &mut self. and..
// because one of the field is String I cannot implement Copy, Clone could work but it might be expensive
pub struct DawsLogs {
    client: Client,
    service: Service,
    log_group: String,
    log_stream: Option<String>,
    job_name: Option<String>,
    last_n_hours: u64,
    filtered_streams: Vec<LogStream>,
}

/*
*s = (*status).into(); // because the target implemented From<&str>, into is generic, rust does not aut-deref for trait dispatch on generics. so dot.
impl From<&str> for GlueJobRunStatus{
    fn from(s: &str) -> Self{
        match s {
            "STARTING" => GlueJobRunStatus::Starting,
            "RUNNING" => GlueJobRunStatus::Running,
            "STOPPING" => GlueJobRunStatus::Stopping,
            "STOPPED" => GlueJobRunStatus::Stopped,
            "SUCCEEDED" => GlueJobRunStatus::Succeeded,
            "FAILED" => GlueJobRunStatus::Failed,
            "TIMEOUT" => GlueJobRunStatus::Timeout,
            _ => GlueJobRunStatus::Na,
        }
    }
}
impl Default for GlueJobRunStatus{
    fn default() -> Self {
        GlueJobRunStatus::Na
    }
}
*/

impl Client {
    async fn new(profile: String) -> Self {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .profile_name(profile)
            .load()
            .await;

        Client {
            sts: sts_client::new(&config),
            logs: logs_client::new(&config),
            glue: glue_client::new(&config),
        }
    }
}

impl DawsLogs {
    pub async fn new() -> Result<Self> {
        let args = Args::parse();
        let client = Client::new(args.profile).await;

        let service = match args.group.as_str() {
            s if s.contains("glue") => Service::Glue,
            s if s.contains("lambda") && args.stream.is_none() => Service::Lambda, // multiple streams, for 1 specified stream -> Service::Generic
            _ => Service::Generic,
        };

        let mut instance = DawsLogs {
            client,
            service,
            log_group: args.group,
            log_stream: args.stream,
            job_name: None::<String>,
            last_n_hours: args.last_n_hours,
            filtered_streams: Vec::new(),
        };
        instance.chk_credentials().await?; // wow - This should be in internal, but I'm leaving it here to know how this can be done.
        instance.chk_log_group().await?;
        if let Some(_) = &instance.log_stream {
            instance.chk_log_stream().await?;
        }

        // Init for service
        match instance.service {
            Service::Glue => {
                instance.set_job_name(instance.get_job_name().await?);
            }
            Service::Lambda => {
                // instance.set_log_stream(instance.get_last_stream().await?);
                let idle_elap = Instant::now();
                loop {
                    if idle_elap.elapsed() > tokio::time::Duration::from_secs(IDLE_LIMIT) {
                        return Err(anyhow!("None LogStream"));
                    }
                    let all_streams = instance.get_all_streams().await?;
                    instance.filtered_streams =
                        instance.filter_streams(all_streams, instance.last_n_hours)?; // unwrap handle when defining the service 
                    if instance.filtered_streams.is_empty() {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        printcr!(
                            "Waiting for some lambda streams within the last {} hours",
                            instance.last_n_hours
                        )
                    } else {
                        break;
                    }
                }
            }
            _ => {}
        }

        /* !Send types if not carefull? // :todo
        cfg(debug_assertions)
        /// check Send+Sync Thread Safe, no idea yet
        fn _assert_send_sync<T: Send + Sync>(_: &T) {};
        _assert_send_sync(&instance);
        */
        Ok(instance)
    }

    fn filter_streams(
        &self,
        all_streams: Vec<LogStream>,
        last_n_hours: u64,
    ) -> Result<Vec<LogStream>> {
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?;
        let cutoff = now - tokio::time::Duration::from_secs(last_n_hours * 3600);
        let cutoff_millis = cutoff.as_millis() as i64;

        let r: Vec<LogStream> = all_streams
            .iter()
            .filter(|stream| {
                if let Some(creation_time) = stream.creation_time() {
                    creation_time > cutoff_millis
                } else if let Some(last_event_timestamp) = stream.last_event_timestamp() {
                    last_event_timestamp > cutoff_millis
                } else {
                    false
                }
            })
            .cloned()
            .collect();
        // println!("Streams Filtered last {last_n_hours} hours: {}", r.len());
        Ok(r)
    }

    async fn get_all_streams(&self) -> Result<Vec<LogStream>> {
        let mut all_streams = Vec::new();
        let mut next_token = None::<String>;
        loop {
            let r = self
                .client
                .logs
                .describe_log_streams()
                .log_group_name(&self.log_group)
                .order_by(logs_OrderBy::LastEventTime)
                .descending(true)
                .set_next_token(next_token.clone())
                .send()
                .await?;

            // println!("[DEBUG] i:{i} {:#?}",r);
            // for stream in r.log_streams(){
            //     println!("Stream x {:#?}",stream)
            // }
            // let y =r.log_streams();
            all_streams.extend_from_slice(r.log_streams());

            next_token = r.next_token().map(|s| s.to_string()); // Option<&str> to Option<String>
            // println!("all streams! {:#?}",all_streams);
            if next_token.is_none() {
                break;
            }
        }
        Ok(all_streams)
    }

    fn set_job_name(&mut self, name: Option<String>) {
        self.job_name = name;
    }

    async fn update_tracker_with_new_logstreams(
        &self,
        mut tracker: StreamTracker,
    ) -> Result<StreamTracker> {
        let all_streams = self.get_all_streams().await?;
        let filtered_streams = self.filter_streams(all_streams, self.last_n_hours)?;
        for stream in &filtered_streams {
            if let Some(name) = stream.log_stream_name() {
                tracker.entry(name.to_string()).or_insert(StreamState {
                    next_token: None::<String>,
                });
            }
        }
        return Ok(tracker);
    }

    async fn get_lambda_logs(&self) -> Result<()> {
        let mut dot = "".to_string();

        let mut tracker: StreamTracker = HashMap::new();
        for stream in &self.filtered_streams {
            if let Some(name) = stream.log_stream_name() {
                tracker.insert(
                    name.to_string(),
                    StreamState {
                        next_token: None::<String>,
                    },
                );
            }
        }
        let mut idle_elap = Instant::now();
        'outer: loop {
            tracker = self
                .update_tracker_with_new_logstreams(tracker.clone())
                .await?;
            for (name, state) in tracker.iter_mut() {
                let r = self
                    .client
                    .logs
                    .get_log_events()
                    .log_group_name(&self.log_group)
                    .log_stream_name(name)
                    .set_next_token(state.next_token.clone())
                    .limit(1000)
                    .send()
                    .await?;

                let events = r.events();
                if !events.is_empty() {
                    idle_elap = Instant::now();
                    for event in events {
                        let timestamp = event.timestamp().unwrap_or_default() / 1000;
                        let timestamp_str = daws_internal::timestamp_sec_to_et(timestamp);
                        let message = event.message().unwrap_or_default().trim_end();
                        let _ingestion = event.ingestion_time().unwrap_or_default();
                        println!("[{name}] [{timestamp_str}] - {message}");
                    }
                }
                if idle_elap.elapsed() > tokio::time::Duration::from_secs(IDLE_LIMIT) {
                    // println!("[INFO] No new logs in the past {IDLE_LIMIT} sec");
                    break 'outer;
                }
                state.next_token = r.next_forward_token().map(|s| s.to_string()); // Option<String> != Option<&str> -> Option<&str> map to get Some(str)
            }
            if dot == "..." {
                dot = "".to_string();
            } else {
                dot += ".";
            }
            printcr!("{}", dot);
            daws_internal::sleep(1..2).await;
        }

        Ok(())
    }

    pub async fn get_logs(&self) -> Result<()> {
        // let condition_glue = || self.glue_condition(); // both are of type impl Fn() -> Bool, but closures are anynoymous unique types, can't be named, you'll see type {code:line2-3}
        // let condition_generic = || true;
        /*
        let condition:Box<dyn Fn() -> bool> = match self.service {
            Service::Glue => Box::new(|| true), // haven't tested it DawsLogs::glue_condition, it works butI'll have to creat another one forthe generic always_true() and call it like condition(Self).
            _ => Box::new(|| true), // forget about the specific, unique type of this closure, treait is as a pointer to some value that implmenets the Fn() trait
        };
        */
        match self.service {
            Service::Glue => {
                self.get_log_events(async || {
                    Ok(self.chk_glue_job_running().await.unwrap_or_default())
                })
                .await?; // unwrapping into bool -> defaults is false
            }
            Service::Lambda => {
                // self.get_log_events(async || Ok(true)).await?;
                self.get_lambda_logs().await?;
            }
            _ => {
                // self.get_log_events(||true).await?;
                self.get_log_events(async || Ok(true)).await?;
            }
        };

        Ok(())
    }

    async fn get_log_events<F, Fut>(&self, job_running: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<bool>>,
    {
        // F is upper case!
        let mut next_token: Option<String> = None;
        let mut idle_elap = Instant::now();
        let latest_stream = self.log_stream.clone().context("Empty log_stream")?;
        loop {
            let r = self
                .client
                .logs
                .get_log_events()
                .log_group_name(&self.log_group)
                .log_stream_name(&latest_stream)
                .set_next_token(next_token.clone())
                .start_from_head(true)
                .send()
                .await?;

            let events = r.events();
            if !events.is_empty() {
                idle_elap = Instant::now();
                for event in events {
                    let timestamp = event.timestamp().unwrap_or_default() / 1000;
                    let timestamp_str = daws_internal::timestamp_sec_to_et(timestamp);
                    let message = event.message().unwrap_or_default().trim_end();
                    let _ingestion = event.ingestion_time().unwrap_or_default();
                    println!("[{timestamp_str}] - {message}");
                }
            }
            let new_token = r.next_forward_token().map(|s| s.to_string()); // Option<String> != Option<&str> -> Option<&str> map to get Some(str)
            daws_internal::sleep(1..2).await;
            if !job_running().await? || idle_elap.elapsed() > tokio::time::Duration::from_secs(3600)
            {
                break;
            }
            if new_token == next_token {
                if idle_elap.elapsed() > tokio::time::Duration::from_secs(IDLE_LIMIT) {
                    // println!("[INFO] No new logs in the past {IDLE_LIMIT} sec");
                    break;
                }
            }
            next_token = new_token.clone();
        }
        Ok(())
    }

    async fn chk_credentials(&self) -> Result<()> {
        let r = self.client.sts.get_caller_identity().send().await;
        let re = Regex::new(r"(?i)(token|credential)").unwrap();
        match r {
            Ok(r) => {
                let _account = r.account().unwrap_or("<no-account>");
            }
            Err(e) if re.is_match(&format!("{:?}", e)) => {
                return Err(anyhow!("Token/Credential Issue"));
            }
            Err(e) => return Err(anyhow!("[ERROR-0] {e:?}")),
        }
        Ok(())
    }
    /*
    // println!("[ERROR-0] {:#?}", err);
    // match &err{

    // Sts_SdkError::ServiceError (err_1) => println!("[ERROR-1] {err_1:?}"),
    // Sts_SdkError::DispatchFailure(err_2) => {
        // println!("[ERROR-2] {err_2:#?}");
        // let err_3 = &err.source();
        // println!("[ERROR-3] {err_3:#?}");
        // let dyn_err = &err_2 as &dyn Error; // some variant of dispatch failure contain boxed erro types, cast to &dyn Error which exposes .source()
        // let source = dyn_err.source();
        // println!("source:{source:#?}");
        // if let Some(err_4) = &err.source(){
            //     // println!("err_4: {err_4:#?}");
            //     if let Some(err_5) = err_4.downcast_ref::<aws_credential_types::provider::error::CredentialsError>(){
                //         println!("err_5: {err_5:#?}");
                //     }
                // }
                // _ => eprintln!("[ERROR-0] AuthError"),
                // },
                // aws_sdk_sts::Error::ExpiredTokenException (err_1) => println!("gonooo"),
                //     aws_sdk_sts::error::SdkError::ServiceError {err:service_err ,..} =>println!("ish"),
                // }
                */

    /// return true if the job status Running, false for everything else.
    async fn chk_glue_job_running(&self) -> Result<bool> {
        let mut r = false;
        if let Some(glue_job_name) = &self.job_name {
            match self.chk_glue_job_run(&glue_job_name).await? {
                Some(status) if status == "RUNNING" => {
                    r = true;
                    daws_internal::sleep(1..2).await;
                }
                _ => {}
            }
        }
        // println!("[TEST] chk_glue_job_running {r}");
        Ok(r)
    }

    async fn chk_log_group(&self) -> Result<()> {
        let r = self
            .client
            .logs
            .describe_log_groups()
            .log_group_name_prefix(&self.log_group)
            .send()
            .await;

        if r.unwrap().log_groups().is_empty() {
            return Err(anyhow!("[ERROR] Log group doesn't exists"));
        }
        Ok(())
    }

    async fn chk_log_stream(&self) -> Result<()> {
        if let Some(stream) = &self.log_stream {
            let idle_elap = Instant::now();
            if stream.is_empty() {
                return Err(anyhow!("Empty LogStream"));
            }
            let request = self
                .client
                .logs
                .describe_log_streams()
                .log_group_name(&self.log_group)
                .log_stream_name_prefix(stream);

            while request
                .clone()
                .send()
                .await
                .unwrap()
                .log_streams()
                .is_empty()
            {
                crate::printcr!(
                    // crate thanks to #[macro_export]
                    "[INFO] Waiting for {lg}/{ls}",
                    lg = &self.log_group,
                    ls = stream,
                );
                daws_internal::sleep(1..2).await;
                crate::printcr!();
                if idle_elap.elapsed() > tokio::time::Duration::from_secs(IDLE_LIMIT) {
                    return Err(anyhow!("None LogStream"));
                }
            }
        } else {
            return Err(anyhow!("None LogStream"));
        }
        Ok(())
    }

    /// Returns the state of the job run
    async fn chk_glue_job_run(&self, job_name: &String) -> Result<Option<String>> {
        // println!("chk_glue_job_run: {}",job_name);
        let mut r: Option<String> = None;
        let jr_r = self
            .client
            .glue
            .get_job_run()
            .job_name(job_name)
            .run_id(self.log_stream.clone().context("Empty log_stream")?)
            .send()
            .await;

        match jr_r {
            Ok(job_d) => {
                if let Some(job) = job_d.job_run() {
                    r = Some(job.job_run_state().unwrap().to_string());
                    // println!("chk_glue_job_run {:?}",r);
                }
            }
            Err(e) if format!("{e:?}").contains("EntityNotFoundException") => {
                return Err(anyhow!("[ERROR] EntityNotFoundException"));
            }
            Err(e) => return Err(anyhow!("SDK Error {}", e)),
        }
        Ok(r)
    }

    /// Returns the name of the glue job associeted with the job_id (log_stream)
    async fn get_all_jobs(&self) -> Result<Vec<String>> {
        let mut r = vec![];
        let mut paginator = self
            .client
            .glue
            .list_jobs()
            .max_results(1000)
            .into_paginator()
            .send();
        '_outer: while let Some(page_r) = paginator.next().await {
            let page = page_r?;
            for job in page.job_names() {
                r.push(job.clone());
            }
        }
        // println!("Jobs len: {}", r.len());
        Ok(r)
    }

    /// Returns the name of the glue job associeted with the job_id (log_stream)
    async fn get_job_name(&self) -> Result<Option<String>> {
        let jobs: Vec<String> = self.get_all_jobs().await?;
        // futures is more general purpose than tokio::streams
        let results = futures::stream::iter(jobs)
            .map(async move |job| {
                match self.chk_glue_job_run(&job).await {
                    Ok(state) => (job, state), // just to confirmed that input[1] = output[1].0
                    Err(_) => (job, None::<String>), // turbofish to keep both arms same type
                }
            }) // i had to import StreamExt from futures
            .buffer_unordered(CONCURRENCY_LIMIT)
            .collect::<Vec<_>>()
            .await;

        let job_name: Vec<&(String, Option<String>)> =
            results.iter().filter(|(_, opt)| opt.is_some()).collect();
        /*
        println!("{:?}",job_name);
        if let Some(i) = results.iter().position(|x|x.1==job_name[0].1){println!("results[{}] = {:?}",i,results[i])}
        else{println!("[TEST] Verification Not Found")}
        */
        let mut r = None::<String>; // Option<String>
        if !job_name.is_empty() {
            println!("Glue job name: {}", job_name[0].0);
            r = Some(job_name[0].0.clone());
        }
        // let r = job_name.pop().cloned().unwrap_or(("Not Job Name Found".to_string(),None::<String>));
        Ok(r)
    }
}

mod daws_internal {
    // imports need to go here! named module dont have acces to parents module's imports
    // Write is a trait, it implements write::fmt which i need for the loading_print
    // use super::DawsLogs; // if need to import my struct
    use chrono::{TimeZone, Utc};
    use chrono_tz::America::Toronto;
    use core::ops::Range;
    use rand::{Rng, rng};
    use std::io::{Write, stdout};

    pub(super) fn timestamp_sec_to_et(timestamp_sec: i64) -> String {
        let dt_utc = Utc.timestamp_opt(timestamp_sec, 0).unwrap();
        let dt_et = dt_utc.with_timezone(&Toronto);
        dt_et.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    // 2 traits with the same name
    // use std::fmt::Write as FmtWrite;
    // use std::io::Write as IoWrite;

    // #[cfg(not(test))]
    #[doc(hidden)]
    /// prints text with carriage return, if text is empty prints claim
    pub(super) fn _printcr(text: String) {
        let claim = "\x1B[2K".to_string();
        let text = [&text, &claim][text.is_empty() as usize];

        let mut stdout = stdout();
        let padded = format!("{:width$}\r", text, width = text.len()); // I could have claim after variable
        write!(stdout, "{}", padded).unwrap();
        stdout.flush().unwrap();
    }

    #[macro_export]
    /// Prints to standard output, with carriage return. if text is empty or no argument is passed => prints claim
    macro_rules! printcr {
        () => {
            daws_internal::_printcr("".to_string()); // pub mode internl{} if i want outise $crate::daws_logs::
        };
        ($($args:tt)*) => ({
            daws_internal::_printcr(format!( $($args) *));
        });
    }

    /// Sleep for random second in within sleep_range
    pub(super) async fn sleep(sleep_range: Range<u64>) {
        tokio::time::sleep(tokio::time::Duration::from_secs(
            rng().random_range(sleep_range),
        ))
        .await;
    }
}
