// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

//! Setup server

use std::sync::Arc;

use analytic_engine::{
    self,
    setup::{EngineBuilder, ReplicatedEngineBuilder, RocksEngineBuilder},
};
use catalog_impls::{table_based::TableBasedManager, CatalogManagerImpl};
use common_util::runtime;
use df_operator::registry::FunctionRegistryImpl;
use log::info;
use logger::RuntimeLevel;
use query_engine::executor::ExecutorImpl;
use server::{
    config::{Config, RuntimeConfig},
    server::Builder,
    table_engine::{MemoryTableEngine, TableEngineProxy},
};
use table_engine::engine::EngineRuntimes;
use tracing_util::{
    self,
    tracing_appender::{non_blocking::WorkerGuard, rolling::Rotation},
};

use crate::signal_handler;

/// Setup log with given `config`, returns the runtime log level switch.
pub fn setup_log(config: &Config) -> RuntimeLevel {
    server::logger::init_log(config).expect("Failed to init log.")
}

/// Setup tracing with given `config`, returns the writer guard.
pub fn setup_tracing(config: &Config) -> WorkerGuard {
    tracing_util::init_tracing_with_file(
        &config.tracing_log_name,
        &config.tracing_log_dir,
        &config.tracing_level,
        Rotation::NEVER,
    )
}

fn build_runtime(name: &str, threads_num: usize) -> runtime::Runtime {
    runtime::Builder::default()
        .worker_threads(threads_num)
        .thread_name(name)
        .enable_all()
        .build()
        .unwrap_or_else(|e| {
            //TODO(yingwen) replace panic with fatal
            panic!("Failed to create runtime, err:{}", e);
        })
}

fn build_engine_runtimes(config: &RuntimeConfig) -> EngineRuntimes {
    EngineRuntimes {
        read_runtime: Arc::new(build_runtime("ceres-read", config.read_thread_num)),
        write_runtime: Arc::new(build_runtime("ceres-write", config.write_thread_num)),
        meta_runtime: Arc::new(build_runtime("ceres-meta", config.meta_thread_num)),
        bg_runtime: Arc::new(build_runtime("ceres-bg", config.background_thread_num)),
    }
}

/// Run a server, returns when the server is shutdown by user
pub fn run_server(config: Config) {
    // 创建运行时(runtime),运行时类似于一组线程池用于执行异步任务
    // 这里讲线程池分为4种类型,分别是读、写、元数据、后台执行
    let runtimes = Arc::new(build_engine_runtimes(&config.runtime));
    let engine_runtimes = runtimes.clone();

    info!("Server starts up, config:{:#?}", config);

    runtimes.bg_runtime.block_on(async {
        // 根据配置采用不同的可插拔引擎(Pluggable Table Engine)
        if config.analytic.obkv_wal.enable {
            run_server_with_runtimes::<ReplicatedEngineBuilder>(config, engine_runtimes).await;
        } else {
            run_server_with_runtimes::<RocksEngineBuilder>(config, engine_runtimes).await;
        }
    });
}

async fn run_server_with_runtimes<T>(config: Config, runtimes: Arc<EngineRuntimes>)
where
    T: EngineBuilder,
{
    // Build all table engine
    // Create memory engine
    // 创建内存引擎,内存表引擎有专门的引擎
    let memory = MemoryTableEngine;
    // Create analytic engine, 创建存储引擎(大概)
    let analytic_config = config.analytic.clone();
    let analytic_engine_builder = T::default();
    // 根据引擎配置以及运行时对象创建引擎
    let analytic = analytic_engine_builder
        .build(analytic_config, runtimes.clone())
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to setup analytic engine, err:{}", e);
        });

    // Create table engine proxy, 创建table engine代理
    let engine_proxy = Arc::new(TableEngineProxy {
        memory,
        analytic: analytic.clone(),
    });

    // Create catalog manager, use analytic table as backend
    // 创建catalog管理器,catalog管理元数据信息
    let catalog_manager = CatalogManagerImpl::new(
        TableBasedManager::new(analytic, engine_proxy.clone())
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to create catalog manager, err:{}", e);
            }),
    );

    // Init function registry.
    // 函数注册 
    let mut function_registry = FunctionRegistryImpl::new();
    function_registry.load_functions().unwrap_or_else(|e| {
        panic!("Failed to create function registry, err:{}", e);
    });
    let function_registry = Arc::new(function_registry);

    // Create query executor
    // 创建查询执行器
    let query_executor = ExecutorImpl::new();

    // Build and start server
    let mut server = Builder::new(config)
        .runtimes(runtimes.clone())
        .catalog_manager(catalog_manager)
        .query_executor(query_executor)
        .table_engine(engine_proxy)
        .function_registry(function_registry)
        .build()
        .unwrap_or_else(|e| {
            panic!("Failed to create server, err:{}", e);
        });
    server.start().await.unwrap_or_else(|e| {
        panic!("Failed to start server,, err:{}", e);
    });

    // Wait for signal
    signal_handler::wait_for_signal();

    // Stop server
    server.stop();
}
