use bitcask_rs::db;

fn main() {
    let engine = db::Engine::open(Default::default()).expect("failed to open bitcast database");

    engine
        .put("key1".into(), "value1".into())
        .expect("failed to put key-value pair into database");

    println!(
        "got {:?}",
        engine
            .get("key1".into())
            .expect("failed to get key from database")
    );

    engine
        .put("key2".into(), "value2".into())
        .expect("failed to put value-value pair into database");

    println!(
        "got {:?}",
        engine
            .get("key2".into())
            .expect("failed to get key from database")
    );

    engine
        .delete("key1".into())
        .expect("failed to delete key from database");

    println!("got {:?}", engine.get("key1".into()));
}
