import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Sparkles,
  Send,
  Loader2,
  Database,
  ChevronRight,
  ChevronLeft,
  ArrowLeft,
  Table2,
  Code,
  AlertCircle,
  Zap,
  Sun,
  Network,
  CloudSun,
  MessageSquare,
  Clock,
  Activity,
  Trash2,
} from 'lucide-react'

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface QuestionCategory {
  label: string
  questions: string[]
}

interface GenieSpace {
  space_id: string
  title: string
  description: string
  icon: string
  tables: string[]
  question_categories?: QuestionCategory[]
  sample_questions?: string[]
}

interface ChatMessage {
  role: 'user' | 'genie'
  content: string
  sql?: string
  columns?: string[]
  rows?: unknown[][]
  status?: 'pending' | 'running' | 'done' | 'error'
  error?: string
}

// ---------------------------------------------------------------------------
// Icon mapping
// ---------------------------------------------------------------------------

const ICON_MAP: Record<string, React.ElementType> = {
  zap: Zap,
  sun: Sun,
  network: Network,
  'cloud-sun': CloudSun,
}

// ---------------------------------------------------------------------------
// API helpers
// ---------------------------------------------------------------------------

const BASE = ''

async function fetchSpaces(): Promise<GenieSpace[]> {
  const r = await fetch(`${BASE}/api/genie/spaces`)
  if (!r.ok) throw new Error('Failed to fetch spaces')
  const data = await r.json()
  return data.spaces
}

async function startConversation(
  spaceId: string,
  content: string
): Promise<{ conversation_id: string; message_id: string }> {
  const r = await fetch(`${BASE}/api/genie/spaces/${spaceId}/start-conversation`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ content }),
  })
  if (!r.ok) {
    const errBody = await r.text()
    throw new Error(`Failed to start conversation: ${errBody}`)
  }
  const data = await r.json()
  return {
    conversation_id: data.conversation_id,
    message_id: data.message_id,
  }
}

async function sendMessage(
  spaceId: string,
  conversationId: string,
  content: string
): Promise<{ message_id: string }> {
  const r = await fetch(
    `${BASE}/api/genie/spaces/${spaceId}/conversations/${conversationId}/messages`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content }),
    }
  )
  if (!r.ok) {
    const errBody = await r.text()
    throw new Error(`Failed to send message: ${errBody}`)
  }
  return r.json()
}

async function pollMessage(
  spaceId: string,
  conversationId: string,
  messageId: string
): Promise<Record<string, unknown>> {
  const r = await fetch(
    `${BASE}/api/genie/spaces/${spaceId}/conversations/${conversationId}/messages/${messageId}`
  )
  if (!r.ok) throw new Error('Failed to poll message')
  return r.json()
}

async function getQueryResult(
  spaceId: string,
  conversationId: string,
  messageId: string
): Promise<Record<string, unknown>> {
  const r = await fetch(
    `${BASE}/api/genie/spaces/${spaceId}/conversations/${conversationId}/messages/${messageId}/query-result`
  )
  if (!r.ok) throw new Error('Failed to fetch query result')
  return r.json()
}

// ---------------------------------------------------------------------------
// Space selector card
// ---------------------------------------------------------------------------

function SpaceCard({
  space,
  onSelect,
}: {
  space: GenieSpace
  onSelect: () => void
}) {
  const Icon = ICON_MAP[space.icon] || Sparkles
  return (
    <button
      onClick={onSelect}
      className="group text-left bg-white rounded-xl border border-gray-200 hover:border-blue-300 hover:shadow-lg transition-all p-5 flex flex-col gap-3"
    >
      <div className="flex items-center gap-3">
        <div className="w-10 h-10 rounded-lg bg-gradient-to-br from-blue-100 to-blue-200 flex items-center justify-center group-hover:from-blue-200 group-hover:to-blue-300 transition-colors">
          <Icon size={20} className="text-blue-600" />
        </div>
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-semibold text-gray-900 group-hover:text-blue-700 transition-colors">
            {space.title}
          </h3>
        </div>
        <ChevronRight
          size={16}
          className="text-gray-300 group-hover:text-blue-400 transition-colors shrink-0"
        />
      </div>
      <p className="text-xs text-gray-500 leading-relaxed">{space.description}</p>
      <div className="flex flex-wrap gap-1.5 mt-1">
        {space.tables.slice(0, 4).map((t) => (
          <span
            key={t}
            className="inline-flex items-center gap-1 text-[10px] font-mono px-2 py-0.5 rounded-full bg-gray-100 text-gray-500"
          >
            <Database size={9} />
            {t}
          </span>
        ))}
        {space.tables.length > 4 && (
          <span className="text-[10px] text-gray-400 px-1">
            +{space.tables.length - 4} more
          </span>
        )}
      </div>
    </button>
  )
}

// ---------------------------------------------------------------------------
// Results table
// ---------------------------------------------------------------------------

function ResultsTable({ columns, rows }: { columns: string[]; rows: unknown[][] }) {
  if (!columns.length || !rows.length) return null
  const displayRows = rows.slice(0, 100)
  return (
    <div className="mt-3 overflow-x-auto rounded-lg border border-gray-200">
      <table className="min-w-full text-xs border-collapse">
        <thead>
          <tr className="bg-gray-100">
            {columns.map((col) => (
              <th
                key={col}
                className="px-3 py-2 text-left font-semibold text-gray-700 text-[11px] uppercase tracking-wider border-b border-gray-200 whitespace-nowrap"
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100">
          {displayRows.map((row, i) => (
            <tr key={i} className="hover:bg-gray-50 transition-colors">
              {row.map((cell, j) => (
                <td key={j} className="px-3 py-1.5 text-gray-700 whitespace-nowrap">
                  {cell === null ? (
                    <span className="text-gray-300 italic">null</span>
                  ) : (
                    String(cell)
                  )}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {rows.length > 100 && (
        <div className="px-3 py-2 text-xs text-gray-400 bg-gray-50 border-t border-gray-200">
          Showing 100 of {rows.length} rows
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Chat message bubble — matches AI Market Intelligence style with AI/You avatars
// ---------------------------------------------------------------------------

function MessageBubble({ msg }: { msg: ChatMessage }) {
  const isUser = msg.role === 'user'
  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'} mb-4`}>
      {!isUser && (
        <div className="w-7 h-7 rounded-full bg-gradient-to-br from-blue-500 to-blue-700 flex items-center justify-center text-white text-[10px] font-bold mr-2.5 mt-0.5 shrink-0 shadow-sm">
          AI
        </div>
      )}
      <div
        className={[
          'max-w-[85%] rounded-2xl',
          isUser
            ? 'bg-blue-600 text-white rounded-br-sm px-4 py-2.5 text-[13px] leading-relaxed'
            : 'bg-white border border-gray-200 text-gray-800 rounded-bl-sm px-4 py-3 shadow-sm',
        ].join(' ')}
      >
        {/* Status indicator */}
        {msg.status === 'pending' && (
          <div className="flex items-center gap-2 text-xs text-gray-400 mb-1">
            <Loader2 size={12} className="animate-spin" />
            Starting conversation...
          </div>
        )}
        {msg.status === 'running' && (
          <div className="flex items-center gap-2 text-xs text-blue-500 mb-1">
            <Loader2 size={12} className="animate-spin" />
            Genie is thinking...
          </div>
        )}

        {/* Text content */}
        {msg.content && (
          <p className={`text-[13px] leading-relaxed ${isUser ? '' : 'text-gray-700'}`}>
            {msg.content}
          </p>
        )}

        {/* Error */}
        {msg.error && (
          <div className="flex items-center gap-2 mt-2 text-xs text-red-500">
            <AlertCircle size={12} />
            {msg.error}
          </div>
        )}

        {/* SQL query */}
        {msg.sql && (
          <details className="mt-3">
            <summary className="flex items-center gap-1.5 text-xs text-gray-400 cursor-pointer hover:text-gray-600">
              <Code size={12} />
              View SQL query
            </summary>
            <pre className="mt-2 text-[11px] bg-gray-800 text-gray-100 rounded-lg px-3 py-2 overflow-x-auto font-mono leading-relaxed">
              {msg.sql}
            </pre>
          </details>
        )}

        {/* Results table */}
        {msg.columns && msg.rows && (
          <div className="mt-2">
            <div className="flex items-center gap-1.5 text-xs text-gray-400 mb-1">
              <Table2 size={12} />
              {msg.rows.length} row{msg.rows.length !== 1 ? 's' : ''} returned
            </div>
            <ResultsTable columns={msg.columns} rows={msg.rows} />
          </div>
        )}
      </div>
      {isUser && (
        <div className="w-7 h-7 rounded-full bg-gray-200 flex items-center justify-center text-gray-500 text-[10px] font-bold ml-2.5 mt-0.5 shrink-0">
          You
        </div>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sidebar — categorized questions + session stats
// ---------------------------------------------------------------------------

function Sidebar({
  space,
  onSelectQuestion,
  messageCount,
  queriesRun,
  sidebarOpen,
  onToggle,
}: {
  space: GenieSpace
  onSelectQuestion: (q: string) => void
  messageCount: number
  queriesRun: number
  sidebarOpen: boolean
  onToggle: () => void
}) {
  const categories = space.question_categories || []
  // Fallback: if no categories but has sample_questions, wrap them in one category
  const displayCategories =
    categories.length > 0
      ? categories
      : space.sample_questions
        ? [{ label: 'Suggested', questions: space.sample_questions }]
        : []

  if (!sidebarOpen) return null

  return (
    <aside className="w-[240px] shrink-0 flex flex-col border-l border-gray-200 bg-gray-50 overflow-hidden">
      {/* Space info */}
      <div className="px-3 py-3 border-b border-gray-200">
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
          Genie Space
        </div>
        <div className="bg-white rounded-lg border border-gray-200 px-3 py-2.5 space-y-1.5">
          <div className="flex items-center gap-1.5">
            <Sparkles size={12} className="text-blue-500 shrink-0" />
            <span className="text-xs font-semibold text-gray-800 leading-tight">
              Databricks AI/BI Genie
            </span>
          </div>
          <div className="text-[11px] text-gray-400 leading-snug">{space.title}</div>
          <div className="flex items-center gap-1.5 pt-0.5">
            <span className="w-2 h-2 rounded-full shrink-0 bg-green-500" />
            <span className="text-xs font-medium text-green-600">Online</span>
          </div>
        </div>
      </div>

      {/* Example questions — categorised & scrollable */}
      <div className="px-3 py-3 border-b border-gray-200 flex-1 overflow-y-auto">
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
          Example Questions
        </div>
        <div className="flex flex-col gap-3">
          {displayCategories.map((cat) => (
            <div key={cat.label}>
              <div className="text-[10px] font-semibold text-gray-400 uppercase tracking-wider mb-1">
                {cat.label}
              </div>
              <div className="flex flex-col gap-1">
                {cat.questions.map((q) => (
                  <button
                    key={q}
                    onClick={() => onSelectQuestion(q)}
                    className="text-left text-[11px] px-2 py-1.5 rounded-md border border-gray-150 bg-white text-gray-600 hover:bg-blue-50 hover:text-blue-700 hover:border-blue-200 transition-colors leading-snug"
                  >
                    {q}
                  </button>
                ))}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Session stats */}
      <div className="px-3 py-3">
        <div className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
          Session Stats
        </div>
        <div className="space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1 text-xs text-gray-500">
              <MessageSquare size={11} />
              <span>Messages</span>
            </div>
            <span className="text-xs font-semibold text-gray-700">{messageCount}</span>
          </div>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-1 text-xs text-gray-500">
              <Activity size={11} />
              <span>Queries run</span>
            </div>
            <span className="text-xs font-semibold text-gray-700">{queriesRun}</span>
          </div>
        </div>
      </div>
    </aside>
  )
}

// ---------------------------------------------------------------------------
// Chat view for a single space — matches AI Market Intelligence layout
// ---------------------------------------------------------------------------

function ChatView({
  space,
  onBack,
}: {
  space: GenieSpace
  onBack: () => void
}) {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [input, setInput] = useState('')
  const [sending, setSending] = useState(false)
  const [conversationId, setConversationId] = useState<string | null>(null)
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [queriesRun, setQueriesRun] = useState(0)
  const scrollRef = useRef<HTMLDivElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const Icon = ICON_MAP[space.icon] || Sparkles

  // Auto-scroll to bottom
  useEffect(() => {
    scrollRef.current?.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' })
  }, [messages])

  // Focus input on mount
  useEffect(() => {
    textareaRef.current?.focus()
  }, [])

  // Cmd/Ctrl+K — focus the input
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
        e.preventDefault()
        textareaRef.current?.focus()
      }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [])

  const updateGenieMsg = useCallback(
    (idx: number, update: Partial<ChatMessage>) => {
      setMessages((prev) => {
        const copy = [...prev]
        copy[idx] = { ...copy[idx], ...update }
        return copy
      })
    },
    []
  )

  const extractAttachments = useCallback(
    async (
      result: Record<string, unknown>,
      convId: string,
      messageId: string
    ): Promise<Partial<ChatMessage>> => {
      const attachments = result.attachments as Record<string, unknown>[] | undefined
      let textContent = ''
      let sql = ''
      let columns: string[] = []
      let rows: unknown[][] = []

      if (attachments && attachments.length > 0) {
        for (const att of attachments) {
          if (att.text) {
            const t = att.text as Record<string, unknown>
            textContent += (t.content as string) || ''
          }
          if (att.query) {
            const q2 = att.query as Record<string, unknown>
            sql = (q2.query as string) || ''
            setQueriesRun((prev) => prev + 1)
            try {
              const qr = await getQueryResult(space.space_id, convId, messageId)
              const statement = qr.statement_response as Record<string, unknown> | undefined
              if (statement) {
                const manifest = statement.manifest as Record<string, unknown> | undefined
                const resultData = statement.result as Record<string, unknown> | undefined
                if (manifest?.schema) {
                  const schema = manifest.schema as Record<string, unknown>
                  const cols = schema.columns as { name: string }[] | undefined
                  columns = cols?.map((c) => c.name) || []
                }
                if (resultData?.data_array) {
                  rows = resultData.data_array as unknown[][]
                }
              }
            } catch {
              // Query result not available yet
            }
          }
        }
      }

      if (!textContent && !sql) {
        textContent = 'I processed your question but have no results to show.'
      }

      return {
        content: textContent,
        sql: sql || undefined,
        columns: columns.length ? columns : undefined,
        rows: rows.length ? rows : undefined,
        status: 'done' as const,
      }
    },
    [space.space_id]
  )

  const handleSend = useCallback(
    async (question: string) => {
      if (!question.trim() || sending) return
      const q = question.trim()
      setInput('')
      setSending(true)

      // Add user message + pending genie message
      const genieIdx = messages.length + 1
      setMessages((prev) => [
        ...prev,
        { role: 'user', content: q },
        { role: 'genie', content: '', status: 'pending' },
      ])

      try {
        let convId = conversationId
        let messageId: string

        if (!convId) {
          updateGenieMsg(genieIdx, { status: 'running' })
          const resp = await startConversation(space.space_id, q)
          convId = resp.conversation_id
          messageId = resp.message_id
          setConversationId(convId)
        } else {
          updateGenieMsg(genieIdx, { status: 'running' })
          const resp = await sendMessage(space.space_id, convId, q)
          messageId = resp.message_id
        }

        // Poll for completion (up to 2 minutes)
        let result: Record<string, unknown> | null = null
        for (let i = 0; i < 60; i++) {
          await new Promise((r) => setTimeout(r, 2000))
          result = await pollMessage(space.space_id, convId!, messageId)
          const status = result.status as string | undefined
          if (status === 'COMPLETED' || status === 'FAILED') break
        }

        const status = result?.status as string | undefined

        if (status === 'FAILED') {
          updateGenieMsg(genieIdx, {
            status: 'error',
            content: 'Genie could not answer this question.',
            error: (result?.error as string) || 'Query failed',
          })
          setSending(false)
          return
        }

        const genieResult = await extractAttachments(result!, convId!, messageId)
        updateGenieMsg(genieIdx, genieResult)
      } catch (err) {
        updateGenieMsg(genieIdx, {
          status: 'error',
          content: '',
          error: err instanceof Error ? err.message : 'Unknown error',
        })
      } finally {
        setSending(false)
      }
    },
    [conversationId, messages.length, sending, space.space_id, updateGenieMsg, extractAttachments]
  )

  const handleClearChat = useCallback(() => {
    setMessages([])
    setConversationId(null)
    setQueriesRun(0)
    setInput('')
    setSending(false)
  }, [])

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
      e.preventDefault()
      void handleSend(input)
    }
  }

  const handleInput = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setInput(e.target.value)
    const el = e.target
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, 160)}px`
  }

  // Gather all questions for the empty state
  const allQuestions =
    space.question_categories
      ? space.question_categories.flatMap((c) => c.questions).slice(0, 6)
      : space.sample_questions?.slice(0, 6) || []

  const isEmpty = messages.length === 0

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Header row — matches AI Market Intelligence header */}
      <div className="px-6 py-3 border-b border-gray-200 bg-white shrink-0 flex items-center justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <button
            onClick={onBack}
            className="p-1.5 rounded-lg hover:bg-gray-100 transition-colors text-gray-400 hover:text-gray-600 shrink-0"
          >
            <ArrowLeft size={18} />
          </button>
          <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-100 to-blue-200 flex items-center justify-center shrink-0">
            <Icon size={16} className="text-blue-600" />
          </div>
          <div className="min-w-0">
            <h2 className="text-xl font-bold text-gray-900 truncate">{space.title}</h2>
            <p className="text-xs text-gray-400 mt-0.5 truncate">
              AI/BI Genie &middot; ask anything in plain English &middot; Cmd+K to focus
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          {/* Query count badge */}
          <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full bg-gray-100 border border-gray-200 text-xs text-gray-600">
            <Activity size={12} className="text-blue-500 shrink-0" />
            <span>
              <span className="font-semibold">{queriesRun}</span>
              <span className="text-gray-400 ml-0.5">queries</span>
            </span>
          </div>
          <button
            onClick={handleClearChat}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium text-gray-600 border border-gray-200 bg-white hover:bg-gray-50 hover:text-red-600 hover:border-red-200 transition-colors"
          >
            <Trash2 size={13} />
            Clear Chat
          </button>
        </div>
      </div>

      {/* Body: chat + collapsible sidebar */}
      <div className="flex flex-1 min-h-0 overflow-hidden">
        {/* Chat area */}
        <div className="flex-1 min-w-0 flex flex-col relative bg-gray-50">
          {/* Messages */}
          <div ref={scrollRef} className="flex-1 overflow-y-auto px-5 py-5">
            {isEmpty ? (
              <div className="flex flex-col items-center justify-center h-full px-6 py-12 text-center">
                <div className="w-12 h-12 rounded-full bg-gradient-to-br from-blue-100 to-blue-200 flex items-center justify-center mb-4">
                  <Sparkles size={22} className="text-blue-500" />
                </div>
                <h3 className="text-base font-semibold text-gray-800 mb-1">
                  {space.title}
                </h3>
                <p className="text-sm text-gray-400 max-w-xs mb-6">
                  Ask anything about your NEM data. Genie will write and execute SQL automatically.
                </p>
                <div className="flex flex-col gap-2 w-full max-w-sm">
                  {allQuestions.map((q) => (
                    <button
                      key={q}
                      onClick={() => handleSend(q)}
                      disabled={sending}
                      className="text-left text-sm px-4 py-2.5 rounded-xl border border-blue-100 bg-blue-50/60 text-blue-700 hover:bg-blue-100 transition-colors disabled:opacity-50"
                    >
                      {q}
                    </button>
                  ))}
                </div>
              </div>
            ) : (
              messages.map((msg, i) => <MessageBubble key={i} msg={msg} />)
            )}
          </div>

          {/* Sidebar toggle button */}
          <button
            onClick={() => setSidebarOpen((o) => !o)}
            aria-label={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
            className="absolute top-3 right-2 z-10 p-1 rounded-md bg-white border border-gray-200 text-gray-400 hover:text-gray-600 hover:bg-gray-50 transition-colors shadow-sm"
          >
            {sidebarOpen ? <ChevronRight size={14} /> : <ChevronLeft size={14} />}
          </button>

          {/* Input area — matches AI Market Intelligence */}
          <div className="border-t border-gray-200 px-5 py-3 bg-white shrink-0">
            <div className="flex items-end gap-2 bg-gray-50 rounded-xl border border-gray-200 px-3 py-2 focus-within:border-blue-400 focus-within:ring-1 focus-within:ring-blue-400 transition-all">
              <textarea
                ref={textareaRef}
                value={input}
                onChange={handleInput}
                onKeyDown={handleKeyDown}
                placeholder="Ask about NEM data — prices, generation, forecasts, constraints... (Cmd+Enter to send)"
                rows={1}
                disabled={sending}
                className="flex-1 resize-none bg-transparent text-sm text-gray-800 placeholder-gray-400 focus:outline-none max-h-40 leading-relaxed disabled:opacity-50"
              />
              <button
                onClick={() => void handleSend(input)}
                disabled={!input.trim() || sending}
                className="shrink-0 p-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                aria-label="Send message"
              >
                {sending ? (
                  <Loader2 size={16} className="animate-spin" />
                ) : (
                  <Send size={16} />
                )}
              </button>
            </div>
            <p className="text-[11px] text-gray-400 mt-1.5 text-center">
              Cmd+Enter to send &middot; Powered by Databricks AI/BI Genie &middot; Queries run against Unity Catalog gold tables
            </p>
          </div>
        </div>

        {/* Collapsible sidebar */}
        <Sidebar
          space={space}
          onSelectQuestion={(q) => handleSend(q)}
          messageCount={messages.filter((m) => m.role === 'user').length}
          queriesRun={queriesRun}
          sidebarOpen={sidebarOpen}
          onToggle={() => setSidebarOpen((o) => !o)}
        />
      </div>
    </div>
  )
}

// ---------------------------------------------------------------------------
// Main Genie page
// ---------------------------------------------------------------------------

export default function Genie() {
  const [spaces, setSpaces] = useState<GenieSpace[]>([])
  const [loading, setLoading] = useState(true)
  const [activeSpace, setActiveSpace] = useState<GenieSpace | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchSpaces()
      .then((s) => {
        setSpaces(s)
        setLoading(false)
      })
      .catch((e) => {
        setError(e.message)
        setLoading(false)
      })
  }, [])

  if (activeSpace) {
    return <ChatView space={activeSpace} onBack={() => setActiveSpace(null)} />
  }

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Header */}
      <div className="px-6 py-3 border-b border-gray-200 bg-white shrink-0">
        <div className="flex items-center gap-2.5">
          <Sparkles size={20} className="text-blue-500" />
          <div>
            <h2 className="text-xl font-bold text-gray-900">Genie Analytics</h2>
            <p className="text-xs text-gray-400 mt-0.5">
              AI-powered data exploration — ask natural language questions against your NEM data
            </p>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 min-h-0 overflow-y-auto px-6 py-6">
        {loading && (
          <div className="flex items-center justify-center h-full">
            <Loader2 size={24} className="animate-spin text-blue-400" />
          </div>
        )}

        {error && (
          <div className="flex flex-col items-center justify-center h-full text-center">
            <AlertCircle size={32} className="text-red-300 mb-3" />
            <p className="text-sm text-red-500">{error}</p>
          </div>
        )}

        {!loading && !error && (
          <>
            <h3 className="text-xs font-semibold text-gray-400 uppercase tracking-wider mb-4">
              Choose a Genie Space
            </h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 max-w-4xl">
              {spaces.map((space) => (
                <SpaceCard
                  key={space.space_id}
                  space={space}
                  onSelect={() => setActiveSpace(space)}
                />
              ))}
            </div>

            {/* Info */}
            <div className="mt-8 max-w-4xl bg-blue-50/60 border border-blue-100 rounded-xl px-5 py-4">
              <div className="flex items-start gap-3">
                <Sparkles size={16} className="text-blue-400 shrink-0 mt-0.5" />
                <div className="text-xs text-gray-600 leading-relaxed">
                  <span className="font-semibold text-blue-700">How it works: </span>
                  Select a Genie space, then ask questions in plain English. Genie uses AI to
                  generate SQL queries against your Unity Catalog gold tables, executes them on your
                  SQL warehouse, and returns the results — all within this interface.
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  )
}
