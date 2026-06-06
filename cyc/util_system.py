def is_notebook() -> bool:
    """True only in a Jupyter kernel, which can render HTML inline. An IPython
    terminal or plain Python returns False (callers should open a browser instead)."""
    try:
        return get_ipython().__class__.__name__ == "ZMQInteractiveShell"  # type: ignore[name-defined]
    except NameError:
        return False
