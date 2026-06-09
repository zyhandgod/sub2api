import { ref } from 'vue'

interface FileSystemFileEntry {
  isFile: true
  isDirectory: false
  file: (successCallback: (file: File) => void, errorCallback?: (error: DOMException) => void) => void
}

interface FileSystemDirectoryReader {
  readEntries: (
    successCallback: (entries: FileSystemEntry[]) => void,
    errorCallback?: (error: DOMException) => void
  ) => void
}

interface FileSystemDirectoryEntry {
  isFile: false
  isDirectory: true
  createReader: () => FileSystemDirectoryReader
}

type FileSystemEntry = FileSystemFileEntry | FileSystemDirectoryEntry

const isJsonFile = (file: File): boolean => {
  const name = file.name.toLowerCase()
  return name.endsWith('.json') || file.type === 'application/json'
}

const readAllDirectoryEntries = async (
  reader: FileSystemDirectoryReader
): Promise<FileSystemEntry[]> => {
  const entries: FileSystemEntry[] = []

  while (true) {
    const batch = await new Promise<FileSystemEntry[]>((resolve, reject) => {
      reader.readEntries(resolve, reject)
    })
    if (batch.length === 0) break
    entries.push(...batch)
  }

  return entries
}

const getFileFromEntry = async (entry: FileSystemFileEntry): Promise<File> => {
  return await new Promise<File>((resolve, reject) => {
    entry.file(resolve, reject)
  })
}

const collectFilesFromEntry = async (entry: FileSystemEntry): Promise<File[]> => {
  if (entry.isFile) {
    const file = await getFileFromEntry(entry)
    return isJsonFile(file) ? [file] : []
  }

  const reader = entry.createReader()
  const entries = await readAllDirectoryEntries(reader)
  const nestedFiles = await Promise.all(entries.map(collectFilesFromEntry))
  return nestedFiles.flat()
}

const uniqueFiles = (files: File[]): File[] => {
  const seen = new Set<string>()
  const out: File[] = []

  for (const file of files) {
    const key = `${file.name}:${file.size}:${file.lastModified}`
    if (seen.has(key)) continue
    seen.add(key)
    out.push(file)
  }

  return out
}

export function useJsonFileDrop() {
  const dragActive = ref(false)

  const collectFromFileList = (fileList: FileList | File[] | null | undefined): File[] => {
    return uniqueFiles(Array.from(fileList ?? []).filter(isJsonFile))
  }

  const collectFromDataTransfer = async (
    dataTransfer: DataTransfer | null | undefined
  ): Promise<File[]> => {
    if (!dataTransfer) return []

    const items = Array.from(dataTransfer.items ?? [])
    const entries = items
      .map((item) => {
        const withEntry = item as unknown as {
          webkitGetAsEntry?: () => FileSystemEntry | null
        }
        return withEntry.webkitGetAsEntry?.() ?? null
      })
      .filter((entry): entry is FileSystemEntry => entry !== null)

    if (entries.length > 0) {
      const files = await Promise.all(entries.map(collectFilesFromEntry))
      return uniqueFiles(files.flat())
    }

    return collectFromFileList(dataTransfer.files)
  }

  return {
    dragActive,
    collectFromFileList,
    collectFromDataTransfer
  }
}
