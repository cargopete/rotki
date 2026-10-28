from PyInstaller.utils.hooks import collect_dynamic_libs

# wasmtime loads its shared library from beside its own package with ctypes.
binaries = collect_dynamic_libs('wasmtime')
