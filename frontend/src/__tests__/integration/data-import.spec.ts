import { describe, it, expect, vi, beforeEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import ImportDataModal from '@/components/admin/account/ImportDataModal.vue'
import { adminAPI } from '@/api/admin'

const showError = vi.fn()
const showSuccess = vi.fn()
const showWarning = vi.fn()

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({
    showError,
    showSuccess,
    showWarning
  })
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      importData: vi.fn(),
      importCodexSession: vi.fn()
    }
  }
}))

vi.mock('vue-i18n', () => ({
  useI18n: () => ({
    t: (key: string) => key
  })
}))

const waitFor = async (predicate: () => boolean) => {
  for (let i = 0; i < 10; i += 1) {
    await flushPromises()
    if (predicate()) return
  }
}

const mountModal = () =>
  mount(ImportDataModal, {
    props: { show: true },
    global: {
      stubs: {
        BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' }
      }
    }
  })

const makeJsonFile = (name: string, content: string, type = 'application/json') => {
  const file = new File([content], name, { type })
  Object.defineProperty(file, 'text', {
    value: () => Promise.resolve(content)
  })
  return file
}

const setInputFiles = (element: Element, files: File[]) => {
  Object.defineProperty(element, 'files', {
    value: files,
    configurable: true
  })
}

describe('ImportDataModal', () => {
  beforeEach(() => {
    showError.mockReset()
    showSuccess.mockReset()
    showWarning.mockReset()
    vi.mocked(adminAPI.accounts.importData).mockReset()
    vi.mocked(adminAPI.accounts.importCodexSession).mockReset()
  })

  it('未选择文件时提示错误', async () => {
    const wrapper = mountModal()

    await wrapper.find('form').trigger('submit')

    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportSelectFile')
  })

  it('无效 JSON 时按文件名提示解析失败', async () => {
    const wrapper = mountModal()

    const input = wrapper.find('input[type="file"]')
    setInputFiles(input.element, [makeJsonFile('data.json', 'invalid json')])

    await input.trigger('change')
    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportParseFailedFile')
    expect(adminAPI.accounts.importData).not.toHaveBeenCalled()
  })

  it('不是导出数据的 JSON 按文件名拒绝', async () => {
    const wrapper = mountModal()

    const input = wrapper.find('input[type="file"]')
    setInputFiles(input.element, [makeJsonFile('random.json', JSON.stringify({ name: 'test' }))])

    await input.trigger('change')
    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportInvalidFile')
    expect(adminAPI.accounts.importData).not.toHaveBeenCalled()
  })

  it('无有效 JSON 的选择不清空已有选择', async () => {
    vi.mocked(adminAPI.accounts.importData).mockResolvedValue({
      proxy_created: 0,
      proxy_reused: 0,
      proxy_failed: 0,
      account_created: 1,
      account_failed: 0
    })

    const wrapper = mountModal()
    const input = wrapper.find('input[type="file"]')

    const valid = makeJsonFile(
      'valid.json',
      JSON.stringify({ exported_at: '2026-07-05T00:00:00Z', proxies: [], accounts: [{ name: 'a' }] })
    )
    setInputFiles(input.element, [valid])
    await input.trigger('change')

    setInputFiles(input.element, [new File(['hello'], 'notes.txt', { type: 'text/plain' })])
    await input.trigger('change')
    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportSelectFile')

    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(adminAPI.accounts.importData).toHaveBeenCalledWith({
      data: expect.objectContaining({
        accounts: [{ name: 'a' }]
      }),
      skip_default_group_bind: true
    })
  })

  it('merges multiple selected JSON files before importing', async () => {
    vi.mocked(adminAPI.accounts.importData).mockResolvedValue({
      proxy_created: 0,
      proxy_reused: 0,
      proxy_failed: 0,
      account_created: 2,
      account_failed: 0
    })

    const wrapper = mountModal()

    const input = wrapper.find('input[type="file"]')
    const first = makeJsonFile(
      'first.json',
      JSON.stringify({ exported_at: '2026-07-05T00:00:00Z', proxies: [], accounts: [{ name: 'a' }] })
    )
    const second = makeJsonFile(
      'second.json',
      JSON.stringify({
        exported_at: '2026-07-05T00:00:01Z',
        proxies: [{ proxy_key: 'p' }],
        accounts: [{ name: 'b' }]
      })
    )
    setInputFiles(input.element, [first, second])

    await input.trigger('change')
    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(adminAPI.accounts.importData).toHaveBeenCalledWith({
      data: expect.objectContaining({
        proxies: [{ proxy_key: 'p' }],
        accounts: [{ name: 'a' }, { name: 'b' }]
      }),
      skip_default_group_bind: true
    })
    expect(showSuccess).toHaveBeenCalledWith('admin.accounts.dataImportSuccess')
  })

  it('部分成功时关闭弹窗仍通知父组件刷新', async () => {
    vi.mocked(adminAPI.accounts.importData).mockResolvedValue({
      proxy_created: 0,
      proxy_reused: 0,
      proxy_failed: 0,
      account_created: 1,
      account_failed: 1
    })

    const wrapper = mountModal()
    const input = wrapper.find('input[type="file"]')
    setInputFiles(input.element, [
      makeJsonFile(
        'mixed.json',
        JSON.stringify({
          exported_at: '2026-07-05T00:00:00Z',
          proxies: [],
          accounts: [{ name: 'a' }, { name: 'b' }]
        })
      )
    ])

    await input.trigger('change')
    await wrapper.find('form').trigger('submit')
    await flushPromises()

    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportCompletedWithErrors')
    expect(wrapper.emitted('imported')).toBeUndefined()

    await wrapper.findAll('button.btn-secondary')[1]!.trigger('click')

    expect(wrapper.emitted('imported')).toHaveLength(1)
    expect(wrapper.emitted('close')).toHaveLength(1)
  })

  it('支持选择多个 JSON 文件并合并导入', async () => {
    vi.mocked(adminAPI.accounts.importData).mockResolvedValue({
      proxy_created: 1,
      proxy_reused: 0,
      proxy_failed: 0,
      account_created: 2,
      account_failed: 0
    })

    const wrapper = mountModal()
    const input = wrapper.find('input[type="file"]')
    setInputFiles(input.element, [
      makeJsonFile(
        'a.json',
        JSON.stringify({
          exported_at: '2026-06-01T00:00:00Z',
          proxies: [{ proxy_key: 'proxy-a' }],
          accounts: [{ name: 'account-a' }]
        })
      ),
      makeJsonFile(
        'b.json',
        JSON.stringify({
          exported_at: '2026-06-02T00:00:00Z',
          proxies: [],
          accounts: [{ name: 'account-b' }]
        })
      )
    ])

    await input.trigger('change')
    await waitFor(() => wrapper.text().includes('admin.accounts.dataImportMergedFromFiles'))
    await wrapper.find('form').trigger('submit')
    await waitFor(() => vi.mocked(adminAPI.accounts.importData).mock.calls.length > 0)

    expect(adminAPI.accounts.importData).toHaveBeenCalledWith({
      data: expect.objectContaining({
        proxies: [{ proxy_key: 'proxy-a' }],
        accounts: [{ name: 'account-a' }, { name: 'account-b' }]
      }),
      skip_default_group_bind: true
    })
  })

  it('支持拖拽文件夹并递归收集 JSON 文件', async () => {
    vi.mocked(adminAPI.accounts.importData).mockResolvedValue({
      proxy_created: 0,
      proxy_reused: 0,
      proxy_failed: 0,
      account_created: 1,
      account_failed: 0
    })

    const wrapper = mountModal()
    const jsonFile = makeJsonFile(
      'folder-account.json',
      JSON.stringify({
        exported_at: '2026-06-03T00:00:00Z',
        proxies: [],
        accounts: [{ name: 'folder-account' }]
      })
    )
    const ignoredFile = new File(['ignore'], 'notes.txt', { type: 'text/plain' })
    const makeFileEntry = (file: File) => ({
      isFile: true,
      isDirectory: false,
      file: (resolve: (file: File) => void) => resolve(file)
    })
    const folderEntry = {
      isFile: false,
      isDirectory: true,
      createReader: () => {
        let called = false
        return {
          readEntries: (resolve: (entries: any[]) => void) => {
            if (called) {
              resolve([])
              return
            }
            called = true
            resolve([makeFileEntry(jsonFile), makeFileEntry(ignoredFile)])
          }
        }
      }
    }

    await wrapper.find('.border-dashed').trigger('drop', {
      dataTransfer: {
        items: [
          {
            webkitGetAsEntry: () => folderEntry
          }
        ],
        files: []
      }
    })
    await waitFor(() => wrapper.text().includes('admin.accounts.dataImportMergedFromFiles'))
    await wrapper.find('form').trigger('submit')
    await waitFor(() => vi.mocked(adminAPI.accounts.importData).mock.calls.length > 0)

    expect(adminAPI.accounts.importData).toHaveBeenCalledWith({
      data: expect.objectContaining({
        proxies: [],
        accounts: [{ name: 'folder-account' }]
      }),
      skip_default_group_bind: true
    })
  })

  it('识别 Codex JSON 并走 Codex 导入接口', async () => {
    vi.mocked(adminAPI.accounts.importCodexSession).mockResolvedValue({
      total: 2,
      created: 1,
      updated: 1,
      skipped: 0,
      failed: 0
    })

    const wrapper = mountModal()
    const input = wrapper.find('input[type="file"]')
    setInputFiles(input.element, [
      makeJsonFile('a.json', JSON.stringify({ type: 'codex', tokens: { access_token: 'token-a' } })),
      makeJsonFile('b.json', JSON.stringify({ type: 'codex', tokens: { access_token: 'token-b' } }))
    ])

    await input.trigger('change')
    await waitFor(() => wrapper.text().includes('admin.accounts.dataImportMergedFromFiles'))
    await wrapper.find('form').trigger('submit')
    await waitFor(() => vi.mocked(adminAPI.accounts.importCodexSession).mock.calls.length > 0)

    expect(adminAPI.accounts.importCodexSession).toHaveBeenCalledWith({
      content: JSON.stringify([
        { type: 'codex', tokens: { access_token: 'token-a' } },
        { type: 'codex', tokens: { access_token: 'token-b' } }
      ], null, 2),
      update_existing: true,
      skip_default_group_bind: true
    })
    expect(adminAPI.accounts.importData).not.toHaveBeenCalled()
    expect(showSuccess).toHaveBeenCalledWith('admin.accounts.dataImportSuccess')
  })
})
