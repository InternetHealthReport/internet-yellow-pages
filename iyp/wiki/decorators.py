from functools import wraps


# Decorator for making a method thread safe and fix concurrent pywikibot cache
# accesses
def thread_safe(method):
    @wraps(method)
    def _impl(self, *method_args, **method_kwargs):
        with self.lock:
            res = method(self, *method_args, **method_kwargs)
        return res
    return _impl
