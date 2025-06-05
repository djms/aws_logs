mod daws_logs;

use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let logs = daws_logs::DawsLogs::new().await?;
    logs.get_logs().await?;
    Ok(())
}
