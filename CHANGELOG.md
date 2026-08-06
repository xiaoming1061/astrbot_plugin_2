# Changelog

## v2.2.1

- 命令检测升级：AstrBot 已匹配的注册命令直接放行，私聊无需 / 前缀即可执行命令（群聊仍需满足 AstrBot 唤醒条件，如 @机器人）
- 未注册命令仍以 command_prefixes 前缀（默认 /）兜底放行

## v2.2.0

- 新增群聊多用户合并模式（merge_mode=group）
- 多用户消息合并为带说话人标注的对话流（[昵称] 文本）
- 昵称清洗：压缩空白并移除方括号，避免破坏输出格式

## v2.1.1

- 修复群聊 mention 触发模式失效的问题（is_at_or_wake_command 为事件属性而非方法）
- 修复部分组件 text 为 None 时命令检测崩溃的问题
- 修复群号获取为空字符串时白名单/黑名单匹配失效的问题

## v2.1.0

- 新增 AstrBot 原生图片组件保留
- 支持“图片后补充问题”的连续消息聚合
- 支持多张图片与文本统一交回 AstrBot 原生处理管线
- 不包含图片下载、本地化、格式转换或链接解析

原生图片组件重建方案参考并改编自：
- astrbot_plugin_continuous_message
- https://github.com/aliveriver/astrbot_plugin_continuous_message

相关实现继续遵循 GNU Affero General Public License v3.0。

## v2.0.0

- 将模型调用调整为 AstrBot 原生事件流水线
- 恢复人格、会话历史、长期记忆与工具调用兼容性
- 新增群聊触发模式
- 新增最大缓冲限制

原生事件重建方案参考并改编自：
- astrbot_plugin_continuous_message
- https://github.com/aliveriver/astrbot_plugin_continuous_message

## v1.1.0

- 群聊白名单与黑名单
- 固定结束符立即提交

## v1.0.0

- 连续消息自动聚合
- 保留 AstrBot 会话上下文
- 支持群聊与私聊
- 自动绕过 AstrBot 指令
- 支持配置化管理
- 兼容 NapCat 等常见适配器