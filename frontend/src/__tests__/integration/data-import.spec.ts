import { describe, it, expect, vi, beforeEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import ImportDataModal from '@/components/admin/account/ImportDataModal.vue'
import { adminAPI } from '@/api/admin'

const showError = vi.fn()
const showSuccess = vi.fn()

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({
    showError,
    showSuccess
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

describe('ImportDataModal', () => {
  beforeEach(() => {
    showError.mockReset()
    showSuccess.mockReset()
    vi.mocked(adminAPI.accounts.importData).mockReset()
    vi.mocked(adminAPI.accounts.importCodexSession).mockReset()
  })

  it('未选择文件时提示错误', async () => {
    const wrapper = mount(ImportDataModal, {
      props: { show: true },
      global: {
        stubs: {
          BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' }
        }
      }
    })

    await wrapper.find('form').trigger('submit')
    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportSelectFile')
  })

  it('无效 JSON 时提示解析失败', async () => {
    const wrapper = mount(ImportDataModal, {
      props: { show: true },
      global: {
        stubs: {
          BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' }
        }
      }
    })

    const input = wrapper.find('input[type="file"]')
    const file = new File(['invalid json'], 'data.json', { type: 'application/json' })
    Object.defineProperty(file, 'text', {
      value: () => Promise.resolve('invalid json')
    })
    Object.defineProperty(input.element, 'files', {
      value: [file]
    })

    await input.trigger('change')
    await wrapper.find('form').trigger('submit')
    await waitFor(() => showError.mock.calls.length > 0)

    expect(showError).toHaveBeenCalledWith('admin.accounts.dataImportParseFailed')
  })

  it('支持选择多个 JSON 文件并合并导入', async () => {
    vi.mocked(adminAPI.accounts.importData).mockResolvedValue({
      proxy_created: 1,
      proxy_reused: 0,
      proxy_failed: 0,
      account_created: 2,
      account_failed: 0
    })

    const wrapper = mount(ImportDataModal, {
      props: { show: true },
      global: {
        stubs: {
          BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' },
          Icon: true
        }
      }
    })

    const fileA = new File([
      JSON.stringify({
        exported_at: '2026-06-01T00:00:00Z',
        proxies: [{ proxy_key: 'proxy-a' }],
        accounts: [{ name: 'account-a' }]
      })
    ], 'a.json', { type: 'application/json' })
    const fileB = new File([
      JSON.stringify({
        exported_at: '2026-06-02T00:00:00Z',
        proxies: [],
        accounts: [{ name: 'account-b' }]
      })
    ], 'b.json', { type: 'application/json' })
    const input = wrapper.find('input[type="file"]')
    Object.defineProperty(input.element, 'files', {
      value: [fileA, fileB],
      configurable: true
    })

    await input.trigger('change')
    await waitFor(() => wrapper.text().includes('admin.accounts.dataImportMergedFromFiles'))
    await wrapper.find('form').trigger('submit')
    await waitFor(() => vi.mocked(adminAPI.accounts.importData).mock.calls.length > 0)

    expect(adminAPI.accounts.importData).toHaveBeenCalledWith({
      data: {
        type: undefined,
        version: undefined,
        exported_at: '2026-06-01T00:00:00Z',
        proxies: [{ proxy_key: 'proxy-a' }],
        accounts: [{ name: 'account-a' }, { name: 'account-b' }]
      },
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

    const wrapper = mount(ImportDataModal, {
      props: { show: true },
      global: {
        stubs: {
          BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' },
          Icon: true
        }
      }
    })

    const jsonFile = new File([
      JSON.stringify({
        exported_at: '2026-06-03T00:00:00Z',
        proxies: [],
        accounts: [{ name: 'folder-account' }]
      })
    ], 'folder-account.json', { type: 'application/json' })
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
      data: {
        type: undefined,
        version: undefined,
        exported_at: '2026-06-03T00:00:00Z',
        proxies: [],
        accounts: [{ name: 'folder-account' }]
      },
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

    const wrapper = mount(ImportDataModal, {
      props: { show: true },
      global: {
        stubs: {
          BaseDialog: { template: '<div><slot /><slot name="footer" /></div>' },
          Icon: true
        }
      }
    })

    const fileA = new File([JSON.stringify({ type: 'codex', tokens: { access_token: 'token-a' } })], 'a.json', {
      type: 'application/json'
    })
    const fileB = new File([JSON.stringify({ type: 'codex', tokens: { access_token: 'token-b' } })], 'b.json', {
      type: 'application/json'
    })
    const input = wrapper.find('input[type="file"]')
    Object.defineProperty(input.element, 'files', {
      value: [fileA, fileB],
      configurable: true
    })

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
