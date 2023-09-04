from main import Logger


def test_pg_handler():
    server_logger = Logger(module_name="server_logs")
    server_logger.info("hello loggerS")
    main_logger = Logger(module_name="main_logs")
    main_logger.info("hello main loggerS")
