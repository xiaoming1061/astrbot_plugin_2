# Changelog

## v1.0.0

- 连续消息自动聚合
- 保留 AstrBot 会话上下文
- 支持群聊与私聊
- 自动绕过 AstrBot 指令
- 支持配置化管理
- 兼容 NapCat 等常见适配器

## v1.1.0

- 群聊白名单与黑名单
- 固定结束符立即提交

## v2.0.0

- 将模型调用调整为 AstrBot 原生事件流水线
- 恢复人格、会话历史、长期记忆与工具调用兼容性
- 新增群聊触发模式
- 新增最大缓冲限制

原生事件重建方案参考并改编自：
- astrbot_plugin_continuous_message
- https://github.com/aliveriver/astrbot_plugin_continuous_message

## v2.1.0

- 新增 AstrBot 原生图片组件保留
- 支持“图片后补充问题”的连续消息聚合
- 支持多张图片与文本统一交回 AstrBot 原生处理管线
- 不包含图片下载、本地化、格式转换或链接解析

原生图片组件重建方案参考并改编自：
- astrbot_plugin_continuous_message
- https://github.com/aliveriver/astrbot_plugin_continuous_message

相关实现继续遵循 GNU Affero General Public License v3.0。