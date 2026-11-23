"""跨模块集成测试：验证8-10组真实数据链路的输入→转换→版本化产物→下游消费。"""
import os
import sys
import json as _json
import uuid as _uuid
import pytest

BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BACKEND_DIR)

from app.services.file_intake import FileIntakeService
from app.services.format_parse import FormatParseService
from app.services.layout_restore import LayoutRestoreService
from app.services.table_extract import TableExtractService
from app.services.ocr_engine import OcrEngineService
from app.services.chunk_split import ChunkSplitService
from app.services.entity_recognize import EntityRecognizeService
from app.services.relation_extract import RelationExtractService
from app.services.term_normalize import TermNormalizeService
from app.services.sensitive_filter import SensitiveFilterService
from app.services.vector_embed import VectorEmbedService
from app.services.kg_build import KgBuildService
from app.services.extract_quality import ExtractQualityService
from app.services.annot_review import AnnotReviewService
from app.services.doc_version import DocVersionService
from app.services.extract_lineage import ExtractLineageService
from app.services.result_deliver import ResultDeliverService
from app.services.parse_retry import ParseRetryService
from app.services.base import DATA_ROOT


def _uid(prefix="ID"):
    return f"{prefix}-{_uuid.uuid4().hex[:8].upper()}"


class TestFileIntakeToFormatParse:
    """链路1：文件接入→格式解析"""

    def test_onboard_batch_persists(self):
        batch_key = _uid("BI")
        doc_id = _uid("DOC")
        svc = FileIntakeService()
        result = svc.onboard_batch({
            "批次编号": batch_key, "来源系统": "测试", "文件清单": f"{doc_id}.pdf",
            "文件类型": "PDF", "字节总量": "1024000", "校验值": "", "采集方式": "接口", "接入游标": "0",
        })
        assert result["批次编号"] == batch_key
        # 验证IO产物
        onboard_file = DATA_ROOT / "batches" / "onboard.jsonl"
        assert onboard_file.exists()

    def test_validate_batch_writes(self):
        batch_key = _uid("BI")
        doc_id = _uid("DOC")
        svc = FileIntakeService()
        cksum = svc._compute_checksum(f"{doc_id}.pdf")
        created = svc.onboard_batch({
            "批次编号": batch_key, "来源系统": "测试", "文件清单": f"{doc_id}.pdf",
            "文件类型": "PDF", "字节总量": "1024000", "校验值": cksum, "采集方式": "接口", "接入游标": "0",
        })
        eid = created["id"]
        svc.run_action(eid, "提交校验")
        result = svc.validate_batch(eid)
        assert result["valid"] is True
        validated_file = DATA_ROOT / "batches" / "validated.jsonl"
        assert validated_file.exists()


class TestFormatParseToChunkSplit:
    """链路2：格式解析→分块切分→实体识别"""

    def test_execute_parse_produces_output(self):
        doc_id = _uid("DD")
        svc = FormatParseService()
        task, _ = svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
            "页码范围": "1-10", "解析模式": "full", "输出格式": "jsonl", "超时秒数": "300", "解析器版本": "v2.1.0",
        })
        result = svc.execute_parse(task["id"])
        assert "error" not in result, result.get("error", "")
        assert result["pages"] > 0
        parsed_file = DATA_ROOT / "parsed" / f"{doc_id}_parsed.jsonl"
        assert parsed_file.exists()

    def test_chunk_split_reads_parse(self):
        doc_id = _uid("DD")
        svc = FormatParseService()
        task, _ = svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        svc.execute_parse(task["id"])
        chunk_svc = ChunkSplitService()
        result = chunk_svc.split_from_document(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["chunks"] > 0
        chunk_file = DATA_ROOT / "chunks" / f"{doc_id}_parsed_chunks.jsonl"
        assert chunk_file.exists()

    def test_entity_recognize_reads_chunks(self):
        doc_id = _uid("DD")
        svc = FormatParseService()
        task, _ = svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        entity_svc = EntityRecognizeService()
        result = entity_svc.recognize_from_chunk(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["entities"] > 0
        entity_file = DATA_ROOT / "entities" / f"{doc_id}_parsed_entities.jsonl"
        assert entity_file.exists()

    def test_relation_extract_reads_entities(self):
        doc_id = _uid("DD")
        svc = FormatParseService()
        task, _ = svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        rel_svc = RelationExtractService()
        result = rel_svc.extract_from_entities(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["relations"] > 0
        rel_file = DATA_ROOT / "relations" / f"{doc_id}_parsed_relations.jsonl"
        assert rel_file.exists()

    def test_kg_build_reads_relations(self):
        doc_id = _uid("DD")
        svc = FormatParseService()
        task, _ = svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        kg_svc = KgBuildService()
        result = kg_svc.build_from_relations(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["triples"] > 0


class TestLayoutRestoreToTableOcr:
    """链路3：版式还原→表格抽取/OCR"""

    def test_layout_restore_produces_output(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        layout_svc = LayoutRestoreService()
        result = layout_svc.restore_from_document(doc_id)
        assert "error" not in result, result.get("error", "")
        assert result["pages"] > 0
        layout_file = DATA_ROOT / "layout" / f"{doc_id}_layout.jsonl"
        assert layout_file.exists()

    def test_table_extract_reads_layout(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        table_file = DATA_ROOT / "layout" / f"{doc_id}_tables.jsonl"
        assert table_file.exists(), f"parse should create table regions for {doc_id}"
        LayoutRestoreService().restore_from_document(doc_id)
        table_svc = TableExtractService()
        result = table_svc.extract_from_layout(doc_id)
        assert "error" not in result, result.get("error", "")
        assert result["tables_extracted"] > 0

    def test_ocr_reads_image_regions(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        LayoutRestoreService().restore_from_document(doc_id)
        ocr_svc = OcrEngineService()
        result = ocr_svc.recognize_from_area(doc_id)
        assert "error" not in result, result.get("error", "")
        assert result["images_processed"] > 0


class TestSecondaryLinks:
    """次级数据链路"""

    def test_sensitive_filter_hits(self):
        svc = SensitiveFilterService()
        result = svc.apply_to_text("手机13800138000邮箱test@example.com")
        assert result["hit_count"] > 0, "应命中手机号和邮箱规则"
        assert "***" in result["filtered_text"] or "[已" in result["filtered_text"]

    def test_term_normalize_maps(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        term_svc = TermNormalizeService()
        result = term_svc.normalize_from_entities(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["mappings"] > 0

    def test_vector_embed_reads_chunks(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        vec_svc = VectorEmbedService()
        result = vec_svc.embed_from_chunk(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["chunks"] > 0

    def test_extract_quality_reports(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        quality_svc = ExtractQualityService()
        result = quality_svc.check_from_batch(doc_id)
        assert "error" not in result, result.get("error", "")
        assert result["accuracy"] > 0


class TestDocVersionDelivery:
    """链路：版本快照→结果交付"""

    def test_create_snapshot_and_deliver(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        KgBuildService().build_from_relations(f"{doc_id}_parsed")
        version_svc = DocVersionService()
        snap_result = version_svc.create_snapshot(doc_id, "v1.0")
        assert "error" not in snap_result, snap_result.get("error", "")
        deliver_svc = ResultDeliverService()
        del_result = deliver_svc.deliver_from_snapshot(snap_result["snapshot"]["快照编号"])
        assert "error" not in del_result, del_result.get("error", "")
        assert del_result["total_records"] > 0


class TestLineageReview:
    """血缘与人工复核"""

    def test_lineage_records_chain(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        KgBuildService().build_from_relations(f"{doc_id}_parsed")
        lineage_svc = ExtractLineageService()
        result = lineage_svc.record_from_parse(doc_id)
        assert "error" not in result, result.get("error", "")
        assert result["nodes"] >= 2

    def test_create_review_tasks(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        review_svc = AnnotReviewService()
        result = review_svc.create_review_tasks(doc_id, threshold=0.99)
        assert result["tasks_created"] > 0


class TestParseRetry:
    """重采与中断恢复"""

    def test_retry_from_failures(self):
        batch_key = _uid("BT")
        svc = ParseRetryService()
        result = svc.create_retry_from_failures(batch_key)
        assert "batch_key" in result

    def test_retry_execution(self):
        batch_key = _uid("BT")
        retry_id = _uid("RETRY")
        svc = ParseRetryService()
        task = {
            "重采编号": retry_id, "失败环节": "格式解析",
            "原始批次": batch_key, "失败原因": "测试", "重试次数": "0",
            "重采范围": "DOC-X", "恢复游标": "0", "重采结果": "", "status": "待重采",
        }
        tf = DATA_ROOT / "retries" / f"{batch_key}_retries.jsonl"
        svc._write_jsonl(tf, [task])
        result = svc.execute_retry(retry_id)
        assert result.get("status") != "已放弃", f"不应放弃: {result}"


class TestDataArtifactIntegrity:
    """端到端产物完整性"""

    def test_full_pipeline_artifacts(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        pr = parse_svc.execute_parse(task["id"])
        assert "error" not in pr
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        KgBuildService().build_from_relations(f"{doc_id}_parsed")
        VectorEmbedService().embed_from_chunk(f"{doc_id}_parsed")
        LayoutRestoreService().restore_from_document(doc_id)
        DocVersionService().create_snapshot(doc_id, "v1.0")
        for sub, name in [("parsed", "_parsed"), ("chunks", "_parsed_chunks"),
                          ("entities", "_parsed_entities"), ("relations", "_parsed_relations"),
                          ("graph", "_parsed_graph"), ("vectors", "_parsed_vectors"),
                          ("layout", "_layout"), ("versions", "_vv1.0")]:
            fp = DATA_ROOT / sub / f"{doc_id}{name}.jsonl"
            assert fp.exists(), f"产物文件 {fp} 应存在"


class TestDownstreamConsumption:
    """验证下游模块真实读取上游产物文件"""

    def test_chunk_consumes_parse(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        parsed_file = DATA_ROOT / "parsed" / f"{doc_id}_parsed.jsonl"
        assert parsed_file.exists()
        result = ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["chunks"] > 0

    def test_entities_consumes_chunk(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        result = EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["entities"] > 0

    def test_kg_consumes_relation(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        ChunkSplitService().split_from_document(f"{doc_id}_parsed")
        EntityRecognizeService().recognize_from_chunk(f"{doc_id}_parsed")
        RelationExtractService().extract_from_entities(f"{doc_id}_parsed")
        result = KgBuildService().build_from_relations(f"{doc_id}_parsed")
        assert "error" not in result, result.get("error", "")
        assert result["triples"] > 0


class TestDataQuality:
    """数据质量验证"""

    def test_entity_dedup(self):
        entities = [
            {"entity_id": "E1", "entity_type": "ORG", "entity_text": "X", "confidence": 0.80},
            {"entity_id": "E2", "entity_type": "ORG", "entity_text": "X", "confidence": 0.95},
        ]
        deduped = EntityRecognizeService()._deduplicate_entities(entities)
        assert len(deduped) == 1
        assert deduped[0]["confidence"] == 0.95

    def test_relation_dedup(self):
        relations = [
            {"relation_id": "R1", "head_entity": "A", "relation_type": "X", "tail_entity": "B", "confidence": 0.8},
            {"relation_id": "R2", "head_entity": "A", "relation_type": "X", "tail_entity": "B", "confidence": 0.9},
        ]
        deduped = RelationExtractService()._deduplicate_relations(relations)
        assert len(deduped) == 1
        assert deduped[0]["confidence"] == 0.9

    def test_empty_parse_safe(self):
        result = ChunkSplitService().split_from_document("NONEXISTENT")
        assert "error" in result, "空分支应安全返回error"

    def test_checkpoint_persistence(self):
        doc_id = _uid("DD")
        parse_svc = FormatParseService()
        task, _ = parse_svc.create_entry({
            "任务编号": _uid("PR"), "文档编号": doc_id, "解析器类型": "PDF",
        })
        parse_svc.execute_parse(task["id"])
        cp_file = DATA_ROOT / "checkpoints" / f"parse_{doc_id}.json"
        assert cp_file.exists(), f"检查点文件 {cp_file} 应存在"