use bytes::Bytes;
use tempfile::Builder;

use crate::{
    db::Engine,
    error::Errors,
    options::Options,
    utils::rand_kv::{get_test_key, get_test_value},
};

#[test]
fn test_engine_put() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    assert!(engine.put(get_test_key(100), get_test_value(100)).is_ok());
    let res = engine.get(get_test_key(100));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(100));

    assert!(engine.put(get_test_key(101), get_test_value(101)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(101));
    assert!(engine.put(get_test_key(101), get_test_value(201)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(201));

    assert!(engine.put(get_test_key(102), Bytes::new()).is_ok());
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);

    assert_eq!(
        engine.put(Bytes::new(), get_test_value(103)),
        Err(Errors::EmptyKey)
    );

    for i in 200..=1000000 {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    drop(engine);

    let engine = Engine::open(opts).expect("failed to open engine");

    assert!(engine.put(get_test_key(105), get_test_value(105)).is_ok());
    let res = engine.get(get_test_key(105));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(105));
}

#[test]
fn test_engine_get() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(100), get_test_value(100)).is_ok());
    let res = engine.get(get_test_key(100));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(100));

    assert!(engine.put(get_test_key(100), get_test_value(101)).is_ok());
    let res = engine.get(get_test_key(100));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(101));

    assert_eq!(engine.delete(get_test_key(100)), Ok(()));
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    drop(engine);

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(101), get_test_value(101)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(101));
    assert!(engine.put(get_test_key(101), get_test_value(1201)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(1201));

    assert!(engine.put(get_test_key(102), Bytes::new()).is_ok());
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);

    assert_eq!(
        engine.put(Bytes::new(), get_test_value(103)),
        Err(Errors::EmptyKey)
    );

    for i in 200..=1000000 {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(1201));
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);

    drop(engine);

    let engine = Engine::open(opts).expect("failed to open engine");

    assert!(engine.put(get_test_key(105), get_test_value(105)).is_ok());
    let res = engine.get(get_test_key(105));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(105));

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(1201));
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);
}

#[test]
fn test_engine_delete() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));
    assert_eq!(engine.delete(get_test_key(100)), Ok(()));
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(100), get_test_value(100)).is_ok());
    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(100)));

    assert_eq!(engine.delete(get_test_key(100)), Ok(()));
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert_eq!(engine.delete(Bytes::new()), Err(Errors::EmptyKey));

    assert!(engine.put(get_test_key(100), get_test_value(201)).is_ok());
    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(201)));

    assert!(engine.put(get_test_key(101), get_test_value(202)).is_ok());
    assert_eq!(engine.get(get_test_key(101)), Ok(get_test_value(202)));
    assert_eq!(engine.delete(get_test_key(101)), Ok(()));
    assert_eq!(engine.get(get_test_key(101)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(102), get_test_value(202)).is_ok());
    assert_eq!(engine.get(get_test_key(102)), Ok(get_test_value(202)));

    for i in 200..=1000000 {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(201)));
    assert_eq!(engine.get(get_test_key(101)), Err(Errors::KeyNotFound));
    assert_eq!(engine.get(get_test_key(102)), Ok(get_test_value(202)));
    assert_eq!(engine.delete(get_test_key(102)), Ok(()));
    assert_eq!(engine.get(get_test_key(102)), Err(Errors::KeyNotFound));

    drop(engine);

    let engine = Engine::open(opts).expect("failed to open engine");

    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(201)));
    assert_eq!(engine.get(get_test_key(101)), Err(Errors::KeyNotFound));
    assert_eq!(engine.get(get_test_key(102)), Err(Errors::KeyNotFound));
}
