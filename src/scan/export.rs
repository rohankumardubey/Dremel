use arrow::datatypes::Schema;
use arrow::ipc::writer::FileWriter;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct IpcExport {
    writer: Option<FileWriter<File>>,
    temporary: PathBuf,
    destination: PathBuf,
}

impl IpcExport {
    pub fn new(path: &Path, schema: &Schema) -> Result<Self, String> {
        if path.try_exists().map_err(|error| error.to_string())? {
            return Err(format!("output {} already exists", path.display()));
        }
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        for _ in 0..100 {
            let temporary = parent.join(format!(
                ".dremel-scan-{}-{}.arrow.tmp",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
            {
                Ok(file) => {
                    let writer = match FileWriter::try_new(file, schema) {
                        Ok(writer) => writer,
                        Err(error) => {
                            let _ = std::fs::remove_file(&temporary);
                            return Err(error.to_string());
                        }
                    };
                    return Ok(Self {
                        writer: Some(writer),
                        temporary,
                        destination: path.to_owned(),
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(format!("cannot create scan export: {error}")),
            }
        }
        Err("cannot allocate a unique scan export path".into())
    }

    pub fn commit(mut self) -> Result<(), String> {
        let mut writer = self.writer.take().expect("export has a writer");
        writer.finish().map_err(|error| error.to_string())?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| error.to_string())?;
        drop(writer);
        // Publishing a hard link is atomic and refuses an existing destination,
        // including one created by another process after the initial check.
        std::fs::hard_link(&self.temporary, &self.destination)
            .map_err(|error| format!("cannot publish {}: {error}", self.destination.display()))?;
        Ok(())
    }

    pub fn write(&mut self, batch: &arrow::record_batch::RecordBatch) -> Result<(), String> {
        self.writer
            .as_mut()
            .expect("export has a writer")
            .write(batch)
            .map_err(|error| error.to_string())
    }
}

impl Drop for IpcExport {
    fn drop(&mut self) {
        drop(self.writer.take());
        let _ = std::fs::remove_file(&self.temporary);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::ipc::reader::FileReader;
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn publishes_only_complete_files_and_preserves_existing_destinations() {
        let directory =
            std::env::temp_dir().join(format!("dremel-scan-export-{}", std::process::id()));
        std::fs::create_dir(&directory).unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2])) as arrow::array::ArrayRef,
        )])
        .unwrap();
        let destination = directory.join("output.arrow");
        {
            let export = IpcExport::new(&destination, &batch.schema()).unwrap();
            let temporary = export.temporary.clone();
            drop(export);
            assert!(!temporary.exists());
            assert!(!destination.exists());
        }
        let mut export = IpcExport::new(&destination, &batch.schema()).unwrap();
        export.write(&batch).unwrap();
        export.commit().unwrap();
        assert_eq!(
            FileReader::try_new(File::open(&destination).unwrap(), None)
                .unwrap()
                .next()
                .unwrap()
                .unwrap()
                .num_rows(),
            2
        );
        assert!(IpcExport::new(&destination, &batch.schema()).is_err());
        let another = directory.join("race.arrow");
        let export = IpcExport::new(&another, &batch.schema()).unwrap();
        std::fs::write(&another, b"original").unwrap();
        assert!(export.commit().is_err());
        assert_eq!(std::fs::read(&another).unwrap(), b"original");
        std::fs::remove_file(destination).unwrap();
        std::fs::remove_file(another).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }
}
