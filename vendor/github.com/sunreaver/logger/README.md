# logger
Packaging the go.uber.org/zap Logger

## 使用说明

必须首先使用 `InitLoggerWithConfig` or `InitLoggerWithLevel` or `InitLogger` 初始化logger模块

**logger.LoggerByDay 按日分割log打印**

**logger.GetLogger(name string) 生成or获取一个name命名的logger**
