from pangaeapy.exporter.pan_exporter import PanExporter


class PanRoCrateExporter(PanExporter):

    def __init__(self, *args, **kwargs):
        super(PanRoCrateExporter, self).__init__(*args, **kwargs)

    def verify(self):
        return True

    def get_rocrate_metadata(self):
        schemameta = self.pandataset.getMetadata('application/ld+json')
        print("SCXHEMA METADATA: ", schemameta)
        metadata = {
            "@context": "https://w3id.org/ro/crate/1.3/context",
            "@graph": [
                {
                  "@type": "CreativeWork",
                  "@id": "ro-crate-metadata.json",
                  "conformsTo": {"@id": "https://w3id.org/ro/crate/1.3"},
                  "about": {"@id": "./"},
                  "description": "RO-Crate Metadata Descriptor"
                }
            ]
        }

        return metadata