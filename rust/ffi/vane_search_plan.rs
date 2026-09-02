use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use lance::index::scalar::load_segments;
use lance::index::DatasetIndexExt;
use lance::Dataset;
use lance_table::format::IndexMetadata;

const FORMAT_VERSION: u16 = 1;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchKind {
    Vector = 0,
    Fts = 1,
    Hybrid = 2,
}

impl TryFrom<u8> for SearchKind {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Vector),
            1 => Ok(Self::Fts),
            2 => Ok(Self::Hybrid),
            _ => bail!("unknown distributed search kind {value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct FrozenIndexMetadata {
    uuid: [u8; 16],
    fields: Vec<i32>,
    name: String,
    dataset_version: u64,
    fragment_ids: Option<Vec<u32>>,
    details: Option<(String, Vec<u8>)>,
    index_version: i32,
    created_at_millis: Option<i64>,
    base_id: Option<u32>,
    files: Option<Vec<(String, u64)>>,
}

impl FrozenIndexMetadata {
    fn from_metadata(metadata: &IndexMetadata) -> Self {
        let mut fields = metadata.fields.clone();
        fields.sort_unstable();
        fields.dedup();

        let fragment_ids = metadata.fragment_bitmap.as_ref().map(|bitmap| {
            let mut ids = bitmap.iter().collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        });
        let details = metadata
            .index_details
            .as_ref()
            .map(|details| (details.type_url.clone(), details.value.clone()));
        // Lance persists IndexMetadata::created_at as timestamp milliseconds.
        // A just-created coordinator handle can still retain the original
        // nanoseconds in memory, while a worker reopening the same manifest
        // observes the persisted millisecond precision. Freeze the canonical
        // persisted representation so both handles identify the same segment.
        let created_at_millis = metadata
            .created_at
            .as_ref()
            .map(|value| value.timestamp_millis());
        let files = metadata.files.as_ref().map(|files| {
            let mut result = files
                .iter()
                .map(|file| (file.path.clone(), file.size_bytes))
                .collect::<Vec<_>>();
            result.sort();
            result
        });

        Self {
            uuid: *metadata.uuid.as_bytes(),
            fields,
            name: metadata.name.clone(),
            dataset_version: metadata.dataset_version,
            fragment_ids,
            details,
            index_version: metadata.index_version,
            created_at_millis,
            base_id: metadata.base_id,
            files,
        }
    }

    fn encode(&self, writer: &mut Writer) -> Result<()> {
        writer.raw(&self.uuid);
        writer.vec(&self.fields, |writer, field| {
            writer.i32(*field);
            Ok(())
        })?;
        writer.string(&self.name)?;
        writer.u64(self.dataset_version);
        writer.option(self.fragment_ids.as_ref(), |writer, ids| {
            writer.vec(ids, |writer, id| {
                writer.u32(*id);
                Ok(())
            })
        })?;
        writer.option(self.details.as_ref(), |writer, (type_url, value)| {
            writer.string(type_url)?;
            writer.bytes(value)
        })?;
        writer.i32(self.index_version);
        writer.option(self.created_at_millis.as_ref(), |writer, millis| {
            writer.i64(*millis);
            Ok(())
        })?;
        writer.option(self.base_id.as_ref(), |writer, base_id| {
            writer.u32(*base_id);
            Ok(())
        })?;
        writer.option(self.files.as_ref(), |writer, files| {
            writer.vec(files, |writer, (path, size)| {
                writer.string(path)?;
                writer.u64(*size);
                Ok(())
            })
        })
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self> {
        let mut uuid = [0_u8; 16];
        uuid.copy_from_slice(reader.raw(16)?);
        let fields = reader.vec(|reader| reader.i32())?;
        let name = reader.string()?;
        let dataset_version = reader.u64()?;
        let fragment_ids = reader.option(|reader| reader.vec(|reader| reader.u32()))?;
        let details = reader.option(|reader| Ok((reader.string()?, reader.bytes()?)))?;
        let index_version = reader.i32()?;
        let created_at_millis = reader.option(|reader| reader.i64())?;
        let base_id = reader.option(|reader| reader.u32())?;
        let files =
            reader.option(|reader| reader.vec(|reader| Ok((reader.string()?, reader.u64()?))))?;
        Ok(Self {
            uuid,
            fields,
            name,
            dataset_version,
            fragment_ids,
            details,
            index_version,
            created_at_millis,
            base_id,
            files,
        })
    }

    fn differing_fields(&self, other: &Self) -> Vec<&'static str> {
        let mut fields = Vec::new();
        if self.uuid != other.uuid {
            fields.push("uuid");
        }
        if self.fields != other.fields {
            fields.push("fields");
        }
        if self.name != other.name {
            fields.push("name");
        }
        if self.dataset_version != other.dataset_version {
            fields.push("dataset_version");
        }
        if self.fragment_ids != other.fragment_ids {
            fields.push("fragment_ids");
        }
        if self.details != other.details {
            fields.push("details");
        }
        if self.index_version != other.index_version {
            fields.push("index_version");
        }
        if self.created_at_millis != other.created_at_millis {
            fields.push("created_at_millis");
        }
        if self.base_id != other.base_id {
            fields.push("base_id");
        }
        if self.files != other.files {
            fields.push("files");
        }
        fields
    }
}

#[derive(Clone, Debug, PartialEq)]
struct BranchPlan {
    field_id: i32,
    field_name: String,
    use_index: bool,
    selected: Vec<FrozenIndexMetadata>,
    covered_fragments: Vec<u64>,
    uncovered_fragments: Vec<u64>,
}

impl BranchPlan {
    fn encode(&self, writer: &mut Writer) -> Result<()> {
        writer.i32(self.field_id);
        writer.string(&self.field_name)?;
        writer.u8(self.use_index.into());
        writer.vec(&self.selected, |writer, metadata| metadata.encode(writer))?;
        writer.vec(&self.covered_fragments, |writer, id| {
            writer.u64(*id);
            Ok(())
        })?;
        writer.vec(&self.uncovered_fragments, |writer, id| {
            writer.u64(*id);
            Ok(())
        })
    }

    fn decode(reader: &mut Reader<'_>) -> Result<Self> {
        let field_id = reader.i32()?;
        let field_name = reader.string()?;
        let use_index = reader.bool()?;
        let selected = reader.vec(FrozenIndexMetadata::decode)?;
        let covered_fragments = reader.vec(|reader| reader.u64())?;
        let uncovered_fragments = reader.vec(|reader| reader.u64())?;
        Ok(Self {
            field_id,
            field_name,
            use_index,
            selected,
            covered_fragments,
            uncovered_fragments,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SearchIndexPlan {
    dataset_version: u64,
    generation: String,
    fragments: Vec<u64>,
    vector: Option<BranchPlan>,
    fts: Option<BranchPlan>,
}

impl SearchIndexPlan {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let mut writer = Writer::default();
        writer.u16(FORMAT_VERSION);
        writer.u64(self.dataset_version);
        writer.string(&self.generation)?;
        writer.vec(&self.fragments, |writer, id| {
            writer.u64(*id);
            Ok(())
        })?;
        writer.option(self.vector.as_ref(), |writer, branch| branch.encode(writer))?;
        writer.option(self.fts.as_ref(), |writer, branch| branch.encode(writer))?;
        Ok(writer.finish())
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        let version = reader.u16()?;
        if version != FORMAT_VERSION {
            bail!("unsupported SearchIndexPlan format version {version}");
        }
        let result = Self {
            dataset_version: reader.u64()?,
            generation: reader.string()?,
            fragments: reader.vec(|reader| reader.u64())?,
            vector: reader.option(BranchPlan::decode)?,
            fts: reader.option(BranchPlan::decode)?,
        };
        reader.finish()?;
        result.validate_shape()?;
        Ok(result)
    }

    fn validate_shape(&self) -> Result<()> {
        if self.dataset_version == 0 || self.generation.is_empty() {
            bail!("SearchIndexPlan is missing its fixed snapshot identity");
        }
        if self.vector.is_none() && self.fts.is_none() {
            bail!("SearchIndexPlan has no search branch");
        }
        validate_sorted_unique(&self.fragments, "fragment ids")?;
        for (name, branch) in [("vector", &self.vector), ("fts", &self.fts)] {
            let Some(branch) = branch else {
                continue;
            };
            if branch.field_name.is_empty() {
                bail!("SearchIndexPlan {name} branch has an empty field name");
            }
            validate_sorted_unique(&branch.covered_fragments, "covered fragments")?;
            validate_sorted_unique(&branch.uncovered_fragments, "uncovered fragments")?;
            let covered = branch
                .covered_fragments
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            let uncovered = branch
                .uncovered_fragments
                .iter()
                .copied()
                .collect::<HashSet<_>>();
            if !covered.is_disjoint(&uncovered)
                || covered.union(&uncovered).copied().collect::<HashSet<_>>()
                    != self.fragments.iter().copied().collect::<HashSet<_>>()
            {
                bail!("SearchIndexPlan {name} branch has invalid fragment coverage");
            }
            if branch.use_index != !branch.selected.is_empty() {
                bail!("SearchIndexPlan {name} branch has an inconsistent index decision");
            }
            for metadata in &branch.selected {
                if metadata.uuid == [0; 16]
                    || metadata.name.is_empty()
                    || metadata.dataset_version == 0
                    || metadata.dataset_version > self.dataset_version
                    || !metadata.fields.contains(&branch.field_id)
                {
                    bail!("SearchIndexPlan {name} branch has invalid index metadata");
                }
                validate_sorted_unique(&metadata.fields, "index fields")?;
                let fragment_ids = metadata.fragment_ids.as_ref().ok_or_else(|| {
                    anyhow!("SearchIndexPlan {name} branch has unknown index fragment coverage")
                })?;
                validate_sorted_unique(fragment_ids, "index fragment ids")?;
                if let Some(files) = &metadata.files {
                    if files.windows(2).any(|pair| pair[0] > pair[1]) {
                        bail!("SearchIndexPlan {name} index files are not canonical");
                    }
                }
            }
            if covered_fragments(&branch.selected, &self.fragments) != branch.covered_fragments {
                bail!(
                    "SearchIndexPlan {name} fragment coverage does not match its selected index segments"
                );
            }
            let mut uuids = branch
                .selected
                .iter()
                .map(|item| item.uuid)
                .collect::<Vec<_>>();
            let original = uuids.clone();
            uuids.sort_unstable();
            uuids.dedup();
            if uuids != original {
                bail!("SearchIndexPlan {name} index segments are not canonical");
            }
        }
        Ok(())
    }

    pub(crate) fn validate_admission(
        &self,
        dataset_version: u64,
        generation: &str,
        kind: SearchKind,
        vector_column: Option<&str>,
        text_column: Option<&str>,
        use_vector_index: bool,
    ) -> Result<()> {
        if self.dataset_version != dataset_version {
            bail!("SearchIndexPlan dataset version does not match the admitted search");
        }
        if self.generation != generation {
            bail!("SearchIndexPlan dataset generation does not match the admitted search");
        }

        match kind {
            SearchKind::Vector if self.vector.is_none() || self.fts.is_some() => {
                bail!("SearchIndexPlan branch set does not describe vector search")
            }
            SearchKind::Fts if self.vector.is_some() || self.fts.is_none() => {
                bail!("SearchIndexPlan branch set does not describe FTS")
            }
            SearchKind::Hybrid if self.vector.is_none() || self.fts.is_none() => {
                bail!("SearchIndexPlan branch set does not describe hybrid search")
            }
            _ => {}
        }

        if let Some(branch) = &self.vector {
            validate_admitted_branch_name(branch, vector_column, "vector")?;
            if branch.use_index && !use_vector_index {
                bail!("SearchIndexPlan vector index decision differs from the search arguments");
            }
        }
        if let Some(branch) = &self.fts {
            validate_admitted_branch_name(branch, text_column, "FTS")?;
        }
        Ok(())
    }

    pub(crate) fn fragments(
        &self,
        dataset: &Dataset,
    ) -> Result<Vec<lance_table::format::Fragment>> {
        let actual = sorted_fragment_ids(dataset);
        if actual != self.fragments {
            bail!("SearchIndexPlan fragment set does not match the fixed worker snapshot");
        }
        Ok(dataset.fragments().as_ref().clone())
    }

    pub(crate) async fn validate(
        &self,
        dataset: &Dataset,
        generation: &str,
        kind: SearchKind,
        vector_column: Option<&str>,
        text_column: Option<&str>,
        use_vector_index: bool,
    ) -> Result<ValidatedSearchIndexPlan> {
        self.validate_admission(
            dataset.version_id(),
            generation,
            kind,
            vector_column,
            text_column,
            use_vector_index,
        )?;
        self.fragments(dataset)?;

        // `load_indices` can return only the current logical-index summary.
        // SearchIndexPlan freezes individual segments, so re-read every frozen logical
        // name through the segment-preserving API before comparing UUIDs and
        // metadata.  Newly-created unrelated names remain intentionally
        // ignored.
        let mut selected_names = self
            .vector
            .iter()
            .chain(self.fts.iter())
            .flat_map(|branch| branch.selected.iter().map(|item| item.name.clone()))
            .collect::<Vec<_>>();
        selected_names.sort();
        selected_names.dedup();
        let mut by_uuid = HashMap::new();
        for name in selected_names {
            for metadata in dataset
                .load_indices_by_name(&name)
                .await
                .with_context(|| format!("load worker index segments for {name}"))?
            {
                by_uuid.insert(*metadata.uuid.as_bytes(), metadata);
            }
        }

        let vector_segments = match &self.vector {
            Some(branch) => {
                validate_branch_identity(dataset, branch, vector_column, "vector")?;
                validate_branch_metadata(branch, &by_uuid, &self.fragments)?
            }
            None => Vec::new(),
        };
        let fts_segments = match &self.fts {
            Some(branch) => {
                validate_branch_identity(dataset, branch, text_column, "FTS")?;
                validate_branch_metadata(branch, &by_uuid, &self.fragments)?
            }
            None => Vec::new(),
        };

        Ok(ValidatedSearchIndexPlan {
            fragments: dataset.fragments().as_ref().clone(),
            vector_segments,
            fts_segments,
        })
    }
}

pub(crate) struct ValidatedSearchIndexPlan {
    pub(crate) fragments: Vec<lance_table::format::Fragment>,
    pub(crate) vector_segments: Vec<IndexMetadata>,
    pub(crate) fts_segments: Vec<IndexMetadata>,
}

pub(crate) async fn build_search_index_plan(
    dataset: &Dataset,
    generation: &str,
    kind: SearchKind,
    vector_column: Option<&str>,
    text_column: Option<&str>,
    use_vector_index: bool,
) -> Result<Vec<u8>> {
    if generation.is_empty() {
        bail!("dataset generation must not be empty");
    }
    let fragments = sorted_fragment_ids(dataset);
    let vector = match kind {
        SearchKind::Vector | SearchKind::Hybrid => Some(
            build_vector_branch(
                dataset,
                vector_column.ok_or_else(|| anyhow!("missing vector column"))?,
                use_vector_index,
                &fragments,
            )
            .await?,
        ),
        SearchKind::Fts => None,
    };
    let fts = match kind {
        SearchKind::Fts | SearchKind::Hybrid => Some(
            build_fts_branch(
                dataset,
                text_column.ok_or_else(|| anyhow!("missing text column"))?,
                &fragments,
            )
            .await?,
        ),
        SearchKind::Vector => None,
    };
    SearchIndexPlan {
        dataset_version: dataset.version_id(),
        generation: generation.to_string(),
        fragments,
        vector,
        fts,
    }
    .encode()
}

async fn build_vector_branch(
    dataset: &Dataset,
    field_name: &str,
    use_index: bool,
    fragments: &[u64],
) -> Result<BranchPlan> {
    let field_id = dataset.schema().field_id(field_name)?;
    let selected = if use_index {
        let indices = dataset.load_indices().await?;
        match indices
            .iter()
            .find(|metadata| metadata.fields.contains(&field_id))
        {
            Some(first) => {
                let segments = dataset.load_indices_by_name(&first.name).await?;
                require_known_fragment_coverage(&segments, "vector")?;
                segments
                    .into_iter()
                    .filter(|metadata| metadata_intersects(metadata, fragments))
                    .collect::<Vec<_>>()
            }
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };
    branch_from_metadata(field_id, field_name, selected, fragments)
}

async fn build_fts_branch(
    dataset: &Dataset,
    field_name: &str,
    fragments: &[u64],
) -> Result<BranchPlan> {
    let field_id = dataset.schema().field_id(field_name)?;
    let segments = load_segments(dataset, field_name)
        .await?
        .unwrap_or_default();
    require_known_fragment_coverage(&segments, "FTS")?;
    let selected = segments
        .into_iter()
        .filter(|metadata| metadata_intersects(metadata, fragments))
        .collect::<Vec<_>>();
    branch_from_metadata(field_id, field_name, selected, fragments)
}

fn require_known_fragment_coverage(metadata: &[IndexMetadata], branch: &str) -> Result<()> {
    if metadata
        .iter()
        .any(|metadata| metadata.fragment_bitmap.is_none())
    {
        bail!("cannot freeze {branch} index fragment coverage");
    }
    Ok(())
}

fn branch_from_metadata(
    field_id: i32,
    field_name: &str,
    selected: Vec<IndexMetadata>,
    fragments: &[u64],
) -> Result<BranchPlan> {
    let mut frozen = selected
        .iter()
        .map(FrozenIndexMetadata::from_metadata)
        .collect::<Vec<_>>();
    frozen.sort_by_key(|metadata| metadata.uuid);
    let covered = covered_fragments(&frozen, fragments);
    let covered_set = covered.iter().copied().collect::<HashSet<_>>();
    let uncovered = fragments
        .iter()
        .copied()
        .filter(|id| !covered_set.contains(id))
        .collect::<Vec<_>>();
    let result = BranchPlan {
        field_id,
        field_name: field_name.to_string(),
        use_index: !frozen.is_empty(),
        selected: frozen,
        covered_fragments: covered,
        uncovered_fragments: uncovered,
    };
    if result
        .selected
        .iter()
        .any(|metadata| !metadata.fields.contains(&field_id))
    {
        bail!("selected index metadata does not contain field {field_id}");
    }
    Ok(result)
}

fn validate_admitted_branch_name(
    branch: &BranchPlan,
    expected_name: Option<&str>,
    branch_name: &str,
) -> Result<()> {
    let expected_name =
        expected_name.ok_or_else(|| anyhow!("missing admitted {branch_name} column"))?;
    if branch.field_name != expected_name {
        bail!("SearchIndexPlan {branch_name} field name does not match the admitted search");
    }
    Ok(())
}

fn validate_branch_identity(
    dataset: &Dataset,
    branch: &BranchPlan,
    expected_name: Option<&str>,
    branch_name: &str,
) -> Result<()> {
    let expected_name = expected_name.ok_or_else(|| anyhow!("missing {branch_name} column"))?;
    if branch.field_name != expected_name
        || dataset.schema().field_id(expected_name)? != branch.field_id
    {
        bail!("SearchIndexPlan {branch_name} field identity does not match the worker search");
    }
    Ok(())
}

fn validate_branch_metadata(
    branch: &BranchPlan,
    current: &HashMap<[u8; 16], IndexMetadata>,
    fragments: &[u64],
) -> Result<Vec<IndexMetadata>> {
    let mut result = Vec::with_capacity(branch.selected.len());
    for expected in &branch.selected {
        let actual_metadata = current
            .get(&expected.uuid)
            .ok_or_else(|| anyhow!("required frozen index segment is absent"))?;
        let actual = FrozenIndexMetadata::from_metadata(actual_metadata);
        let differing_fields = expected.differing_fields(&actual);
        if !differing_fields.is_empty() {
            bail!(
                "required frozen index segment metadata changed ({})",
                differing_fields.join(", ")
            );
        }
        result.push(actual_metadata.clone());
    }
    let covered = covered_fragments(&branch.selected, fragments);
    if covered != branch.covered_fragments {
        bail!("frozen index fragment coverage changed");
    }
    Ok(result)
}

fn sorted_fragment_ids(dataset: &Dataset) -> Vec<u64> {
    let mut fragments = dataset
        .iter_fragments()
        .map(|fragment| fragment.id)
        .collect::<Vec<_>>();
    fragments.sort_unstable();
    fragments
}

fn metadata_intersects(metadata: &IndexMetadata, fragments: &[u64]) -> bool {
    metadata.fragment_bitmap.as_ref().is_some_and(|bitmap| {
        fragments
            .iter()
            .any(|fragment| u32::try_from(*fragment).is_ok_and(|id| bitmap.contains(id)))
    })
}

fn covered_fragments(metadata: &[FrozenIndexMetadata], fragments: &[u64]) -> Vec<u64> {
    fragments
        .iter()
        .copied()
        .filter(|fragment| {
            let Ok(id) = u32::try_from(*fragment) else {
                return false;
            };
            metadata.iter().any(|metadata| {
                metadata
                    .fragment_ids
                    .as_ref()
                    .is_some_and(|ids| ids.binary_search(&id).is_ok())
            })
        })
        .collect()
}

fn validate_sorted_unique<T: Ord>(values: &[T], what: &str) -> Result<()> {
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        bail!("SearchIndexPlan {what} are not sorted and unique");
    }
    Ok(())
}

#[derive(Default)]
struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u16(&mut self, value: u16) {
        self.raw(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.raw(&value.to_le_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.raw(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_le_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.raw(&value.to_le_bytes());
    }

    fn bytes(&mut self, value: &[u8]) -> Result<()> {
        self.length(value.len())?;
        self.raw(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<()> {
        self.bytes(value.as_bytes())
    }

    fn length(&mut self, value: usize) -> Result<()> {
        let value = u32::try_from(value).context("SearchIndexPlan value is too large")?;
        self.u32(value);
        Ok(())
    }

    fn vec<T, F>(&mut self, values: &[T], mut encode: F) -> Result<()>
    where
        F: FnMut(&mut Self, &T) -> Result<()>,
    {
        self.length(values.len())?;
        for value in values {
            encode(self, value)?;
        }
        Ok(())
    }

    fn option<T, F>(&mut self, value: Option<&T>, encode: F) -> Result<()>
    where
        F: FnOnce(&mut Self, &T) -> Result<()>,
    {
        match value {
            Some(value) => {
                self.u8(1);
                encode(self, value)
            }
            None => {
                self.u8(0);
                Ok(())
            }
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn finish(&self) -> Result<()> {
        if self.offset != self.bytes.len() {
            bail!("SearchIndexPlan contains trailing bytes");
        }
        Ok(())
    }

    fn raw(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .offset
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| anyhow!("truncated SearchIndexPlan payload"))?;
        let result = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(result)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.raw(1)?[0])
    }

    fn bool(&mut self) -> Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => bail!("invalid SearchIndexPlan boolean"),
        }
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.raw(2)?.try_into()?))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.raw(4)?.try_into()?))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.raw(4)?.try_into()?))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.raw(8)?.try_into()?))
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.raw(8)?.try_into()?))
    }

    fn length(&mut self) -> Result<usize> {
        let len = self.u32()? as usize;
        if len > self.bytes.len().saturating_sub(self.offset) {
            bail!("SearchIndexPlan collection length exceeds the remaining payload");
        }
        Ok(len)
    }

    fn bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        Ok(self.raw(len)?.to_vec())
    }

    fn string(&mut self) -> Result<String> {
        String::from_utf8(self.bytes()?).context("SearchIndexPlan string is not UTF-8")
    }

    fn vec<T, F>(&mut self, mut decode: F) -> Result<Vec<T>>
    where
        F: FnMut(&mut Self) -> Result<T>,
    {
        let len = self.length()?;
        let mut result = Vec::with_capacity(len);
        for _ in 0..len {
            result.push(decode(self)?);
        }
        Ok(result)
    }

    fn option<T, F>(&mut self, decode: F) -> Result<Option<T>>
    where
        F: FnOnce(&mut Self) -> Result<T>,
    {
        match self.u8()? {
            0 => Ok(None),
            1 => Ok(Some(decode(self)?)),
            _ => bail!("invalid SearchIndexPlan option tag"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn branch(field_id: i32, field_name: &str) -> BranchPlan {
        BranchPlan {
            field_id,
            field_name: field_name.to_string(),
            use_index: false,
            selected: Vec::new(),
            covered_fragments: Vec::new(),
            uncovered_fragments: vec![1, 3],
        }
    }

    fn indexed_branch(field_id: i32, field_name: &str, uuid: u8) -> BranchPlan {
        BranchPlan {
            field_id,
            field_name: field_name.to_string(),
            use_index: true,
            selected: vec![FrozenIndexMetadata {
                uuid: [uuid; 16],
                fields: vec![field_id],
                name: format!("{field_name}_idx"),
                dataset_version: 6,
                fragment_ids: Some(vec![1]),
                details: Some(("type.example/index".to_string(), vec![1, 2, 3])),
                index_version: 1,
                created_at_millis: Some(123_456),
                base_id: None,
                files: Some(vec![("index.idx".to_string(), 99)]),
            }],
            covered_fragments: vec![1],
            uncovered_fragments: vec![3],
        }
    }

    fn plan(vector: Option<BranchPlan>, fts: Option<BranchPlan>) -> SearchIndexPlan {
        SearchIndexPlan {
            dataset_version: 7,
            generation: "generation".to_string(),
            fragments: vec![1, 3],
            vector,
            fts,
        }
    }

    #[test]
    fn search_index_plan_round_trip_is_canonical() {
        let plan = plan(Some(branch(4, "vector")), None);
        let first = plan.encode().unwrap();
        let decoded = SearchIndexPlan::decode(&first).unwrap();
        assert_eq!(decoded, plan);
        assert_eq!(decoded.encode().unwrap(), first);
    }

    #[test]
    fn search_index_plan_rejects_truncation_and_trailing_bytes() {
        let plan = plan(None, Some(branch(5, "text")));
        let bytes = plan.encode().unwrap();
        for len in 0..bytes.len() {
            assert!(SearchIndexPlan::decode(&bytes[..len]).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(SearchIndexPlan::decode(&trailing).is_err());
    }

    #[test]
    fn search_index_plan_rejects_noncanonical_fragment_sets() {
        let mut plan = plan(Some(branch(4, "vector")), None);
        plan.fragments = vec![3, 1];
        assert!(SearchIndexPlan::decode(&plan.encode().unwrap()).is_err());
    }

    #[test]
    fn search_index_plan_rejects_branchless_and_admission_mismatches() {
        assert!(SearchIndexPlan::decode(&plan(None, None).encode().unwrap()).is_err());

        let vector =
            SearchIndexPlan::decode(&plan(Some(branch(4, "vector")), None).encode().unwrap())
                .unwrap();
        assert!(vector
            .validate_admission(
                7,
                "generation",
                SearchKind::Vector,
                Some("vector"),
                None,
                false,
            )
            .is_ok());
        assert!(vector
            .validate_admission(
                8,
                "generation",
                SearchKind::Vector,
                Some("vector"),
                None,
                false,
            )
            .is_err());
        assert!(vector
            .validate_admission(
                7,
                "other-generation",
                SearchKind::Vector,
                Some("vector"),
                None,
                false,
            )
            .is_err());
        assert!(vector
            .validate_admission(7, "generation", SearchKind::Fts, None, Some("text"), false,)
            .is_err());
        assert!(vector
            .validate_admission(
                7,
                "generation",
                SearchKind::Vector,
                Some("other-vector"),
                None,
                false,
            )
            .is_err());

        let indexed = SearchIndexPlan::decode(
            &plan(Some(indexed_branch(4, "vector", 1)), None)
                .encode()
                .unwrap(),
        )
        .unwrap();
        assert!(indexed
            .validate_admission(
                7,
                "generation",
                SearchKind::Vector,
                Some("vector"),
                None,
                false,
            )
            .is_err());
    }

    #[test]
    fn search_index_plan_hybrid_round_trip_keeps_independent_branches() {
        let plan = plan(
            Some(indexed_branch(4, "vector", 1)),
            Some(indexed_branch(5, "text", 2)),
        );
        let decoded = SearchIndexPlan::decode(&plan.encode().unwrap()).unwrap();
        assert_eq!(decoded, plan);
        assert_ne!(
            decoded.vector.unwrap().field_id,
            decoded.fts.unwrap().field_id
        );
    }

    #[test]
    fn search_index_plan_rejects_contradictory_or_foreign_index_metadata() {
        let mut duplicate = indexed_branch(4, "vector", 1);
        duplicate.selected.push(duplicate.selected[0].clone());
        assert!(SearchIndexPlan::decode(&plan(Some(duplicate), None).encode().unwrap()).is_err());

        let mut unknown_coverage = indexed_branch(4, "vector", 1);
        unknown_coverage.selected[0].fragment_ids = None;
        assert!(
            SearchIndexPlan::decode(&plan(Some(unknown_coverage), None).encode().unwrap()).is_err()
        );

        let mut foreign_field = indexed_branch(4, "vector", 1);
        foreign_field.selected[0].fields = vec![9];
        assert!(
            SearchIndexPlan::decode(&plan(Some(foreign_field), None).encode().unwrap()).is_err()
        );

        let mut future_index = indexed_branch(4, "vector", 1);
        future_index.selected[0].dataset_version = 8;
        assert!(
            SearchIndexPlan::decode(&plan(Some(future_index), None).encode().unwrap()).is_err()
        );

        let mut overlap = indexed_branch(4, "vector", 1);
        overlap.uncovered_fragments = vec![1, 3];
        assert!(SearchIndexPlan::decode(&plan(Some(overlap), None).encode().unwrap()).is_err());

        let mut contradictory_coverage = indexed_branch(4, "vector", 1);
        contradictory_coverage.covered_fragments = vec![3];
        contradictory_coverage.uncovered_fragments = vec![1];
        assert!(SearchIndexPlan::decode(
            &plan(Some(contradictory_coverage), None).encode().unwrap()
        )
        .is_err());
    }

    #[test]
    fn search_index_plan_rejects_unknown_versions_and_unbounded_lengths() {
        let mut unknown_version = plan(Some(branch(4, "vector")), None).encode().unwrap();
        unknown_version[..2].copy_from_slice(&2_u16.to_le_bytes());
        assert!(SearchIndexPlan::decode(&unknown_version).is_err());

        let mut oversized_generation = Vec::new();
        oversized_generation.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        oversized_generation.extend_from_slice(&7_u64.to_le_bytes());
        oversized_generation.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(SearchIndexPlan::decode(&oversized_generation).is_err());
    }

    #[test]
    fn search_index_plan_arbitrary_bytes_fail_without_panicking() {
        let mut seed = 0x9e37_79b9_u32;
        for len in 0..256 {
            let bytes = (0..len)
                .map(|_| {
                    seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                    (seed >> 24) as u8
                })
                .collect::<Vec<_>>();
            assert!(SearchIndexPlan::decode(&bytes).is_err());
        }
    }
}
