use anisp_clickhouse_store::ClickHouseStore;
use anisp_config::AppConfig;

#[tokio::test]
#[ignore = "requires ClickHouse at the configured URL"]
async fn ping_real_clickhouse() {
    let config = AppConfig::from_env().unwrap();
    let store = ClickHouseStore::new(&config.clickhouse);

    store.ping().await.unwrap();
}
