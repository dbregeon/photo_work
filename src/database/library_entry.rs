use std::{
    ffi::{OsStr, OsString},
    path::PathBuf,
};

use chrono::{Datelike, NaiveDate};
use exif::Exif;

use eyre::{eyre, Context, Error, Result};

use super::catalog_entry::CatalogEntry;

#[derive(PartialEq, Debug)]
pub(crate) struct LibraryEntry {
    pub(super) sha256: String,
    pub(super) path: PathBuf,
}

impl LibraryEntry {
    pub(crate) fn new(sha256: String, path: PathBuf) -> Self {
        Self { sha256, path }
    }

    pub(crate) fn sha256(&self) -> &str {
        &self.sha256
    }

    pub(crate) fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl TryFrom<&CatalogEntry> for LibraryEntry {
    type Error = Error;

    fn try_from(catalog_entry: &CatalogEntry) -> Result<LibraryEntry> {
        let exif: Exif = read_exif(&catalog_entry.path())?;
        let original_date = original_date(&exif)
            .map_err(|e| eyre!("For {}: {}", catalog_entry.path().display(), e))?;

        Ok(Self::new(
            catalog_entry.sha256().to_owned(),
            find_unused_library_path(&catalog_entry.path(), original_date)?,
        ))
    }
}

fn find_unused_library_path(path: &PathBuf, original_date: NaiveDate) -> Result<PathBuf> {
    let file_stem = path.file_stem().ok_or(eyre!("Expected a file stem"))?;
    let extension = path.extension().ok_or(eyre!("Expected a file extension"))?;
    let date_based_path = date_based_path(original_date);

    unused_filename(&date_based_path, &file_stem, &extension)
}

fn unused_filename(base_path: &PathBuf, file_stem: &OsStr, extension: &OsStr) -> Result<PathBuf> {
    let mut result = base_path.to_owned();
    result.push(file_stem);
    result.set_extension(extension);
    let mut names = (1..).map(|i| build_file_name(file_stem, &("_".to_owned() + &i.to_string())));
    while result.exists() {
        if let Some(name) = names.next() {
            result.set_file_name(name);
            result.set_extension(extension);
        } else {
            return Err(eyre!("Can't find an unused file name in the library."));
        }
    }
    Ok(result)
}

fn build_file_name(stem: &OsStr, suffix: &str) -> OsString {
    let mut result = stem.to_owned();
    result.push(suffix);
    result
}

fn date_based_path(date: NaiveDate) -> PathBuf {
    [
        &date.year().to_string(),
        &date.month().to_string(),
        &date.day().to_string(),
    ]
    .iter()
    .collect()
}

fn original_date(exif: &Exif) -> Result<NaiveDate> {
    if let Some(datetime_field) = exif.get_field(exif::Tag::DateTimeOriginal, exif::In::PRIMARY) {
        NaiveDate::parse_from_str(
            &datetime_field
                .value
                .display_as(exif::Tag::DateTimeOriginal)
                .to_string(),
            "%Y-%m-%d %H:%M:%S",
        )
        .wrap_err("Failed to parse DateTimmeOriginal")
    } else {
        Err(eyre!("DateTimeOriginal tag not found"))
    }
}

fn read_exif(path: &PathBuf) -> Result<Exif> {
    let file = std::fs::File::open(path)?;
    let mut bufreader = std::io::BufReader::new(&file);
    let exifreader = exif::Reader::new();
    exifreader
        .read_from_container(&mut bufreader)
        .wrap_err(eyre!("Failed to read exif for {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs::{copy, create_dir_all, remove_dir_all, remove_file, File},
        path::PathBuf,
    };

    use chrono::NaiveDate;

    use serial_test::serial;

    use crate::database::{
        catalog_entry::CatalogEntry,
        library_entry::{date_based_path, original_date, read_exif, LibraryEntry},
    };

    #[test]
    fn try_from_creates_library_entry_from_path() {
        let catalog_entry =
            CatalogEntry::try_from(&given_a_path_for_an_image_with_original_date()).unwrap();
        let entry = LibraryEntry::try_from(&catalog_entry).unwrap();
        assert_eq!(entry.sha256(), catalog_entry.sha256());
    }

    #[test]
    fn try_from_fails_to_create_library_entry_from_non_existant_path() {
        let path = ["/tmp", "test.txt"].iter().collect::<PathBuf>();
        File::create(&path).unwrap();
        let catalog_entry = CatalogEntry::try_from(&path).unwrap();
        remove_file(&path).unwrap();

        assert!(LibraryEntry::try_from(&catalog_entry).is_err());
    }

    #[test]
    fn read_exif_returns_an_error() {
        let path: &PathBuf = &given_a_path_for_non_exif_file();

        assert!(read_exif(path).is_err());
    }

    #[test]
    fn try_from_fails_to_create_library_entry_from_non_exif_file() {
        let catalog_entry = CatalogEntry::try_from(&given_a_path_for_non_exif_file()).unwrap();
        assert!(LibraryEntry::try_from(&catalog_entry).is_err());
    }

    #[test]
    fn date_based_path_uses_slash_separator() {
        let date = NaiveDate::from_ymd_opt(2023, 12, 2).unwrap();
        assert_eq!(
            [2023.to_string(), 12.to_string(), 2.to_string()]
                .iter()
                .collect::<PathBuf>(),
            date_based_path(date)
        );
    }

    #[test]
    fn original_date_returns_the_original_naive_date_from_exif() {
        let path = &given_a_path_for_an_image_with_original_date();
        let exif = read_exif(path).unwrap();

        assert_eq!(
            NaiveDate::from_ymd_opt(2023, 5, 18).unwrap(),
            original_date(&exif).unwrap()
        );
    }

    #[test]
    fn original_date_returns_error() {
        let path = &given_a_path_for_an_image_with_no_original_date();

        let exif = read_exif(path).unwrap();
        assert!(original_date(&exif).is_err());
    }

    #[test]
    #[serial]
    fn library_path_is_from_the_original_date_of_the_image() {
        let path = &given_a_path_for_an_image_with_original_date();
        let catalog_entry = CatalogEntry::try_from(path).unwrap();
        let _ = remove_dir_all(PathBuf::from(2023.to_string()));

        assert_eq!(
            &[
                2023.to_string(),
                5.to_string(),
                18.to_string(),
                path.file_name().unwrap().to_string_lossy().to_string()
            ]
            .iter()
            .collect::<PathBuf>(),
            LibraryEntry::try_from(&catalog_entry).unwrap().path()
        );
    }

    #[test]
    #[serial]
    fn library_path_is_filename_is_changed_when_conflicting() {
        let path = &given_a_path_for_an_image_with_original_date();
        let occupied_path = [
            2023.to_string(),
            5.to_string(),
            18.to_string(),
            path.file_name().unwrap().to_string_lossy().to_string(),
        ]
        .iter()
        .collect::<PathBuf>();

        let _ = remove_dir_all(PathBuf::from(2023.to_string()));
        create_dir_all(&occupied_path.parent().unwrap()).unwrap();
        copy(path, &occupied_path).unwrap();

        let result = LibraryEntry::try_from(&CatalogEntry::try_from(path).unwrap());

        assert_eq!(
            &[
                2023.to_string(),
                5.to_string(),
                18.to_string(),
                path.file_stem().unwrap().to_string_lossy().to_string()
                    + "_1."
                    + &path.extension().unwrap().to_string_lossy().to_string()
            ]
            .iter()
            .collect::<PathBuf>(),
            result.unwrap().path()
        );
    }

    fn given_a_path_for_an_image_with_original_date() -> PathBuf {
        ["resources", "test", "kami_neko.jpeg"].iter().collect()
    }

    fn given_a_path_for_an_image_with_no_original_date() -> PathBuf {
        ["resources", "test", "no_original_date.jpeg"]
            .iter()
            .collect()
    }

    fn given_a_path_for_non_exif_file() -> PathBuf {
        PathBuf::from("Cargo.toml")
    }
}
