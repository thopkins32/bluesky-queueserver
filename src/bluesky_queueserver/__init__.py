from importlib import import_module

from ._version import __version__

_LAZY_EXPORTS = {
    "parameter_annotation_decorator": (".manager.annotation_decorator", "parameter_annotation_decorator"),
    "CommTimeoutError": (".manager.comms", "CommTimeoutError"),
    "ZMQCommSendAsync": (".manager.comms", "ZMQCommSendAsync"),
    "ZMQCommSendThreads": (".manager.comms", "ZMQCommSendThreads"),
    "generate_zmq_keys": (".manager.comms", "generate_zmq_keys"),
    "generate_zmq_public_key": (".manager.comms", "generate_zmq_public_key"),
    "validate_zmq_key": (".manager.comms", "validate_zmq_key"),
    "gen_list_of_plans_and_devices": (".manager.gen_lists", "gen_list_of_plans_and_devices"),
    "ReceiveConsoleOutput": (".manager.output_streaming", "ReceiveConsoleOutput"),
    "ReceiveConsoleOutputAsync": (".manager.output_streaming", "ReceiveConsoleOutputAsync"),
    "ReceiveSystemInfo": (".manager.output_streaming", "ReceiveSystemInfo"),
    "ReceiveSystemInfoAsync": (".manager.output_streaming", "ReceiveSystemInfoAsync"),
    "bind_plan_arguments": (".manager.profile_ops", "bind_plan_arguments"),
    "construct_parameters": (".manager.profile_ops", "construct_parameters"),
    "format_text_descriptions": (".manager.profile_ops", "format_text_descriptions"),
    "register_device": (".manager.profile_ops", "register_device"),
    "register_plan": (".manager.profile_ops", "register_plan"),
    "validate_plan": (".manager.profile_ops", "validate_plan"),
    "clear_ipython_mode": (".manager.profile_tools", "clear_ipython_mode"),
    "clear_re_worker_active": (".manager.profile_tools", "clear_re_worker_active"),
    "is_ipython_mode": (".manager.profile_tools", "is_ipython_mode"),
    "is_re_worker_active": (".manager.profile_tools", "is_re_worker_active"),
    "set_ipython_mode": (".manager.profile_tools", "set_ipython_mode"),
    "set_re_worker_active": (".manager.profile_tools", "set_re_worker_active"),
}

__all__ = ["__version__", *_LAZY_EXPORTS]


def __getattr__(name: str):
    try:
        module_name, attribute_name = _LAZY_EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc
    value = getattr(import_module(module_name, __name__), attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
