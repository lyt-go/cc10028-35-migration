<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">文档解析与知识抽取平台</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向多格式文档批量接入、版式解析、分块切分、实体关系抽取、向量化嵌入与抽取质量校验入库的非结构化知识抽取平台。</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "交付索引 v2", path: "/delivery-index" }, { label: "文件接入", path: "/file_intake" }, { label: "格式解析", path: "/format_parse" }, { label: "版式还原", path: "/layout_restore" }, { label: "表格抽取", path: "/table_extract" }, { label: "OCR识别", path: "/ocr_engine" }, { label: "分块切分", path: "/chunk_split" }, { label: "实体识别", path: "/entity_recognize" }, { label: "关系抽取", path: "/relation_extract" }, { label: "术语归一", path: "/term_normalize" }, { label: "敏感词过滤", path: "/sensitive_filter" }, { label: "向量嵌入", path: "/vector_embed" }, { label: "知识图谱", path: "/kg_build" }, { label: "抽取质量", path: "/extract_quality" }, { label: "人工复核", path: "/annot_review" }, { label: "文档版本", path: "/doc_version" }, { label: "抽取血缘", path: "/extract_lineage" }, { label: "结果交付", path: "/result_deliver" }, { label: "失败重采", path: "/parse_retry" }]
</script>
