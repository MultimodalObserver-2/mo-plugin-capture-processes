from mo.core import Properties
from mo import translate

properties = Properties()

properties.add_int("snapshot_interval", translate("processes_capture.property.snapshot_interval"), min=1, max=3600, step=1, )
properties.set_default("snapshot_interval", 5)

properties.add_bool("export_to_csv", translate("processes_capture.property.export_to_csv"))
properties.set_default("export_to_csv", False)

properties.add_bool("stream_to_clients", translate("processes_capture.property.stream_to_clients"))
properties.set_default("stream_to_clients", True)
