<template>
  <BaseDialog
    :show="show"
    :title="t('admin.accounts.dataImportTitle')"
    width="normal"
    close-on-click-outside
    @close="handleClose"
  >
    <form id="import-data-form" class="space-y-4" @submit.prevent="handleImport">
      <div class="text-sm text-gray-600 dark:text-dark-300">
        {{ t('admin.accounts.dataImportHint') }}
      </div>
      <div
        class="rounded-lg border border-amber-200 bg-amber-50 p-3 text-xs text-amber-600 dark:border-amber-800 dark:bg-amber-900/20 dark:text-amber-400"
      >
        {{ t('admin.accounts.dataImportWarning') }}
      </div>

      <div>
        <label class="input-label">{{ t('admin.accounts.dataImportFile') }}</label>
        <div
          :class="[
            'rounded-lg border-2 border-dashed px-4 py-4 transition-colors',
            dragActive
              ? 'border-primary-500 bg-primary-50 dark:border-primary-500 dark:bg-primary-900/20'
              : 'border-gray-300 bg-gray-50 hover:border-primary-400 hover:bg-primary-50/40 dark:border-dark-600 dark:bg-dark-800 dark:hover:border-primary-600 dark:hover:bg-primary-900/10'
          ]"
          @dragenter.prevent="dragActive = true"
          @dragover.prevent="dragActive = true"
          @dragleave.prevent="dragActive = false"
          @drop.prevent="handleFileDrop"
        >
          <div class="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div class="min-w-0">
              <div class="truncate text-sm font-medium text-gray-900 dark:text-white" :title="fileListTitle">
                {{ selectedFilesLabel || t('admin.accounts.dataImportDropTitle') }}
              </div>
              <div class="mt-1 text-xs text-gray-500 dark:text-dark-400">
                {{ t('admin.accounts.dataImportDropHint') }}
              </div>
              <div v-if="mergedContent" class="mt-1 text-xs text-emerald-600 dark:text-emerald-400">
                {{ t('admin.accounts.dataImportMergedFromFiles', { count: sourceFileCount }) }}
              </div>
            </div>
            <button type="button" class="btn btn-secondary shrink-0" @click="openFilePicker">
              {{ t('common.chooseFile') }}
            </button>
          </div>
        </div>
        <input
          ref="fileInput"
          type="file"
          class="hidden"
          accept="application/json,.json"
          multiple
          @change="handleFileChange"
        />
      </div>

      <div
        v-if="result"
        class="space-y-2 rounded-xl border border-gray-200 p-4 dark:border-dark-700"
      >
        <div class="text-sm font-medium text-gray-900 dark:text-white">
          {{ t('admin.accounts.dataImportResult') }}
        </div>
        <div class="text-sm text-gray-700 dark:text-dark-300">
          {{ t('admin.accounts.dataImportResultSummary', result) }}
        </div>

        <div v-if="errorItems.length" class="mt-2">
          <div class="text-sm font-medium text-red-600 dark:text-red-400">
            {{ t('admin.accounts.dataImportErrors') }}
          </div>
          <div
            class="mt-2 max-h-48 overflow-auto rounded-lg bg-gray-50 p-3 font-mono text-xs dark:bg-dark-800"
          >
            <div v-for="(item, idx) in errorItems" :key="idx" class="whitespace-pre-wrap">
              {{ item.kind }} {{ item.name || item.proxy_key || '-' }} - {{ item.message }}
            </div>
          </div>
        </div>
      </div>
    </form>

    <template #footer>
      <div class="flex justify-end gap-3">
        <button class="btn btn-secondary" type="button" :disabled="importing" @click="handleClose">
          {{ t('common.cancel') }}
        </button>
        <button
          class="btn btn-primary"
          type="submit"
          form="import-data-form"
          :disabled="importing"
        >
          {{ importing ? t('admin.accounts.dataImporting') : t('admin.accounts.dataImportButton') }}
        </button>
      </div>
    </template>
  </BaseDialog>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import BaseDialog from '@/components/common/BaseDialog.vue'
import { adminAPI } from '@/api/admin'
import { useAppStore } from '@/stores/app'
import { useJsonFileDrop } from '@/composables/useJsonFileDrop'
import type { AdminDataImportResult, AdminDataPayload, CodexSessionImportResult } from '@/types'

interface Props {
  show: boolean
}

interface Emits {
  (e: 'close'): void
  (e: 'imported'): void
}

const props = defineProps<Props>()
const emit = defineEmits<Emits>()

const { t } = useI18n()
const appStore = useAppStore()
const { dragActive, collectFromFileList, collectFromDataTransfer } = useJsonFileDrop()

const importing = ref(false)
const sourceFiles = ref<File[]>([])
const sourceFileCount = ref(0)
const mergedContent = ref('')
const mergedFileKind = ref<'standard' | 'codex' | null>(null)
const hasCreatedData = ref(false)
const result = ref<AdminDataImportResult | null>(null)

const fileInput = ref<HTMLInputElement | null>(null)

const selectedFilesLabel = computed(() => {
  if (sourceFiles.value.length === 0) return ''
  if (sourceFiles.value.length === 1) return sourceFiles.value[0]?.name || ''
  return t('admin.accounts.selectedCount', { count: sourceFiles.value.length })
})
const fileListTitle = computed(() => sourceFiles.value.map((item) => item.name).join(', '))
const errorItems = computed(() => result.value?.errors || [])

watch(
  () => props.show,
  (open) => {
    if (open) {
      sourceFiles.value = []
      sourceFileCount.value = 0
      mergedContent.value = ''
      mergedFileKind.value = null
      hasCreatedData.value = false
      result.value = null
      if (fileInput.value) {
        fileInput.value.value = ''
      }
    }
  }
)

const openFilePicker = () => {
  fileInput.value?.click()
}

const handleFileChange = async (event: Event) => {
  const target = event.target as HTMLInputElement
  const incoming = Array.from(target.files || [])
  const nextFiles = collectFromFileList(target.files)
  if (incoming.length > 0 && nextFiles.length < incoming.length) {
    appStore.showWarning(
      t('admin.accounts.dataImportIgnoredFiles', { count: incoming.length - nextFiles.length })
    )
  }
  if (nextFiles.length === 0) {
    appStore.showError(t('admin.accounts.dataImportSelectFile'))
    target.value = ''
    return
  }
  sourceFiles.value = nextFiles
  await prepareMergedFile(nextFiles)
  target.value = ''
}

const handleFileDrop = async (event: DragEvent) => {
  dragActive.value = false
  if (importing.value) return

  const nextFiles = await collectFromDataTransfer(event.dataTransfer)
  if (nextFiles.length === 0) {
    appStore.showError(t('admin.accounts.dataImportSelectFile'))
    return
  }

  sourceFiles.value = nextFiles
  await prepareMergedFile(nextFiles)
  if (fileInput.value) {
    fileInput.value.value = ''
  }
}

const handleClose = () => {
  if (importing.value) return
  if (hasCreatedData.value) {
    hasCreatedData.value = false
    emit('imported')
  }
  emit('close')
}

const readFileAsText = async (sourceFile: File): Promise<string> => {
  if (typeof sourceFile.text === 'function') {
    return sourceFile.text()
  }

  if (typeof sourceFile.arrayBuffer === 'function') {
    const buffer = await sourceFile.arrayBuffer()
    return new TextDecoder().decode(buffer)
  }

  return await new Promise<string>((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => resolve(String(reader.result ?? ''))
    reader.onerror = () => reject(reader.error || new Error('Failed to read file'))
    reader.readAsText(sourceFile)
  })
}

const SUPPORTED_DATA_TYPES = ['sub2api-data', 'sub2api-bundle']
const SUPPORTED_DATA_VERSION = 1

const isValidDataPayload = (payload: unknown): payload is AdminDataPayload => {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return false
  const candidate = payload as Record<string, unknown>
  if (
    candidate.type !== undefined &&
    candidate.type !== '' &&
    !SUPPORTED_DATA_TYPES.includes(candidate.type as string)
  ) {
    return false
  }
  if (
    candidate.version !== undefined &&
    candidate.version !== 0 &&
    candidate.version !== SUPPORTED_DATA_VERSION
  ) {
    return false
  }
  return Array.isArray(candidate.proxies) && Array.isArray(candidate.accounts)
}

const mergeDataPayloads = (payloads: AdminDataPayload[]): AdminDataPayload => {
  const [firstPayload] = payloads
  if (payloads.length === 1 && firstPayload) return firstPayload

  return {
    type: payloads.find((item) => typeof item.type === 'string')?.type,
    version: payloads.find((item) => typeof item.version === 'number')?.version,
    exported_at: new Date().toISOString(),
    proxies: payloads.flatMap((item) => item.proxies),
    accounts: payloads.flatMap((item) => item.accounts),
    skipped_shadows: payloads.reduce((sum, item) => {
      const count = Number(item.skipped_shadows || 0)
      return Number.isFinite(count) ? sum + count : sum
    }, 0)
  }
}

const isCodexPayload = (payload: unknown): boolean => {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    String((payload as { type?: unknown }).type ?? '').trim().toLowerCase() === 'codex'
  )
}

const toDataImportResultFromCodex = (codexResult: CodexSessionImportResult): AdminDataImportResult => {
  return {
    proxy_created: 0,
    proxy_reused: 0,
    proxy_failed: 0,
    account_created: codexResult.created + codexResult.updated,
    account_failed: codexResult.failed,
    errors: (codexResult.errors || []).map((item) => ({
      kind: 'account',
      name: item.name,
      message: `#${item.index}: ${item.message}`
    }))
  }
}

const prepareMergedFile = async (nextFiles: File[]) => {
  result.value = null
  sourceFileCount.value = nextFiles.length
  mergedContent.value = ''
  mergedFileKind.value = null

  const payloads: unknown[] = []
  for (const sourceFile of nextFiles) {
    let parsed: unknown
    try {
      parsed = JSON.parse(await readFileAsText(sourceFile))
    } catch {
      appStore.showError(t('admin.accounts.dataImportParseFailedFile', { name: sourceFile.name }))
      return
    }
    payloads.push(parsed)
  }

  const hasCodexPayload = payloads.some(isCodexPayload)
  const hasStandardPayload = payloads.some((payload) => !isCodexPayload(payload))
  if (hasCodexPayload && hasStandardPayload) {
    appStore.showError(t('admin.accounts.dataImportMixedTypes'))
    return
  }

  if (hasCodexPayload) {
    mergedContent.value = JSON.stringify(payloads, null, 2)
    mergedFileKind.value = 'codex'
    return
  }

  const dataPayloads: AdminDataPayload[] = []
  for (let index = 0; index < payloads.length; index += 1) {
    const parsed = payloads[index]
    const sourceFile = nextFiles[index]
    if (!isValidDataPayload(parsed)) {
      appStore.showError(
        t('admin.accounts.dataImportInvalidFile', { name: sourceFile?.name || '' })
      )
      return
    }
    dataPayloads.push(parsed)
  }

  mergedContent.value = JSON.stringify(mergeDataPayloads(dataPayloads), null, 2)
  mergedFileKind.value = 'standard'
}

const handleImport = async () => {
  if ((!mergedContent.value || !mergedFileKind.value) && sourceFiles.value.length > 0) {
    await prepareMergedFile(sourceFiles.value)
  }
  if (!mergedContent.value || !mergedFileKind.value) {
    appStore.showError(t('admin.accounts.dataImportSelectFile'))
    return
  }

  importing.value = true
  try {
    const res = mergedFileKind.value === 'codex'
      ? toDataImportResultFromCodex(await adminAPI.accounts.importCodexSession({
          content: mergedContent.value,
          update_existing: true,
          skip_default_group_bind: true
        }))
      : await adminAPI.accounts.importData({
          data: JSON.parse(mergedContent.value),
          skip_default_group_bind: true
        })

    result.value = res

    const msgParams: Record<string, unknown> = {
      account_created: res.account_created,
      account_failed: res.account_failed,
      proxy_created: res.proxy_created,
      proxy_reused: res.proxy_reused,
      proxy_failed: res.proxy_failed,
    }
    if (res.account_failed > 0 || res.proxy_failed > 0) {
      if (res.account_created > 0 || res.proxy_created > 0) {
        hasCreatedData.value = true
      }
      appStore.showError(t('admin.accounts.dataImportCompletedWithErrors', msgParams))
    } else {
      appStore.showSuccess(t('admin.accounts.dataImportSuccess', msgParams))
      emit('imported')
    }
  } catch (error: any) {
    if (error instanceof SyntaxError) {
      appStore.showError(t('admin.accounts.dataImportParseFailed'))
    } else {
      appStore.showError(error?.message || t('admin.accounts.dataImportFailed'))
    }
  } finally {
    importing.value = false
  }
}
</script>
