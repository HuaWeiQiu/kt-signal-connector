# KT Signal Connector Implementation Plan

## 1. Status

- Decision date: 2026-08-04
- Current status: Connector Phases 1–3 are implemented locally; the separate KT Desktop Phase 4
  integration is locally merged at `5e18793c`, while production Phase 3 exit gates remain open
- No-revision record: 2026-10-13; the §4.123/§4.124 automatic send-retry queue
  (the official 190 ms → 15 min backoff drain over definitively-failed
  outbound texts, plus the maintained manual-only line for restart-pending
  rows) lands as connector-internal behavior only — zero wire change, the
  contract revision stays 1.136 (the §4.123/§4.124 entries below)
- No-revision record: 2026-10-13; the §4.121/§4.122 receive-side gap pair (the
  self-sent sticker / bodyless pure-quote `sentMessage` sync projection and the
  passive `syncMessage.contacts` consume into the contacts cache) lands as
  connector-internal behavior only — zero wire change, the contract revision stays
  1.136 (the §4.121/§4.122 entries below)
- No-revision stopgap record: 2026-10-12; the §4.119 outbound-receipt resupply stopgap
  (seven-day recency window + viewed-face selection filter + supervisor failure debounce)
  lands as connector-internal behavior only — zero wire change, the contract revision stayed
  1.135 at that batch (since superseded by the 1.136 entry below)
- Contract revision: 1.143 (2026-10-13; the rate-limit family joins the
  §4.123 queue on its own Retry-After beat (§4.139): a 429/413 send failure
  is a definitive engine answer — the server said "wait", not "lost", so the
  unknown-outcome red line never applies — and the server's explicit pause
  IS the schedule, not a manual-only dead end the backoff curve would
  replace with a 190 ms head that hammers again almost immediately. The
  settle face takes the schedule decision as a three-arm enum
  (`SendRetrySchedule::None`/`Backoff`/`RateLimit` — the §4.123 one-bool
  face could not carry "which beat"); both automatic arms share the
  eligibility gate, window, and ceilings verbatim (outbound text row,
  clientRequestId, no attachment descriptors §4.134, attempt < 128, per-
  account queue < 256 — the rate-limit arm is not a budget bypass), and the
  beat is the official `sleepForRateLimitRetryAfterTime` fallback: this
  wire carries no Retry-After value by contract (the engine's rate-limit
  face is the class anchor `(RateLimitExceeded)` alone, no value field —
  `DEFAULT_RETRY_AFTER` = MINUTE is the answer the official fallback gives
  a value-less face, so the one-minute default IS the beat here; the
  official 1 s floor and 24 h-window clamp govern any future value face),
  `RATE_LIMIT_RETRY_AFTER_MS` = 60_000. The §4.124 anchor stands
  (SendCommand.java carries no client-timestamp face; the retry dispatches
  through the ordinary rebuild). Wire additive: zero — no new method,
  event, key, error, capability, or store move (the schedule lives in the
  existing §4.123 columns). Tests 778 → 781 (lib 594 → 596: the store's
  exact Retry-After beat + the shared budget refusal on the new arm, the
  three-arm classification, the service enroll test's rate-limit leg moves
  from the refusal family to its own beat assertion; outbox end to end:
  the fixture's one-shot rate-limit answer enrolls the row at the
  one-minute beat, decisively not the 190 ms curve head), all green) —
  previously 1.142 (2026-10-13; the account's own-ACI carry-through
  (§4.138): the account-level identity the desktop needs for self-addressing
  (the call self-UUID and the like) had no connector face — the §4.24
  boundary records the own ACI as not queryable on either engine's jsonRpc
  surface, and the store's account rows answered the gap with the opaque
  local row id, which is random 32-hex and NOT an ACI (never derivable, and
  now explicitly never substituted). Contract-first additive: the pinned
  engine (b719997) answers no ACI anywhere — finishLink answers `{number}`,
  listAccounts `[{number}]`, and the truth (the presage store's
  `registration_data` `service_ids` aci) has no jsonRpc read face — so the
  connector learns to parse an optional `aci` key beside the number on both
  faces (the finishLink result and the listAccounts entries), normalizes it
  through the canonical dashed-lowercase shape (dashed or bare 32-hex, any
  case; anything else is not an ACI and reads as absence, never a repaired
  guess), and persists it on the account row (additive nullable
  `accounts.aci` column, schema 23→24, nothing backfilled). Honesty rules:
  an absent key is not an unreporting — `None` writes nothing, so a stored
  identity survives later faces without the key; a fresh answer overwrites;
  the wire skips the key entirely while the row holds none. Wire
  additive-only: an optional `aci` key on accounts.list entries and the
  link.finish result (skipped when unknown; the schema leaves response
  results unpinned, so §4.138 is the descriptive home), no new method,
  event, error, capability, or store move. Tests 771 → 778 (lib 589 → 594:
  the canonical-shape ladder and refusals, the None-never-unreports writer,
  the v23→v24 migration, the finish-path persistence under
  answered/absent/garbage faces, the per-entry sync attribution, and the
  two end-to-end harness tests — the answered face riding both wire faces
  with the pinned value, and the omit-build reading as absence), all
  green) —
  previously 1.141 (2026-10-13; the engine.storagePull forwarding
  face (§4.137): the engine's T-9b storage-manifest pull outcome event —
  fired once per pull attempt (the login-time automatic pull, trigger
  `login`, and every `fetchLatest` leg) with the per-family legs answering
  truthfully (`contactsOk`/`groupsOk`) instead of a bare done bit — rides
  the §4.117 account-domain transient lane end to end: the parse arm
  guarantees the faces the engine contract promises (the attribution
  account, the trigger word typed as a string with the value domain
  deliberately unlocked so an engine trigger this connector has not seen
  forwards verbatim, never guessed into a known one, and the two boolean
  legs; a missing or wrong-typed face is the unknown-notification
  protocol error, never a guessed forward), the supervisor watcher and
  the service's resolve-or-drop attribution forward through the same
  `account_transient_forwarded`/`_unattributed` counters, and the host
  frame is `engine.storagePull` `{accountId, trigger, contactsOk,
  groupsOk}`. Wire additive-only: one new host event name, no new method,
  error, capability, or store move. Tests 771 held (lib 589 held; the
  four transient-lane tests grow the storagePull assertions in place,
  the 1.119 extended-tests posture), all green) —
  previously 1.140 (2026-10-13; the batch-S quick-win leg — three
  correctness/parity gaps the send-and-sync-granularity audit surfaced, all
  connector-internal, zero wire change: the §4.134 attachment-caption retry
  guard (a failed attachment row carrying a non-empty caption never enters
  the §4.123 queue again — the rebuild's text/quote/preview face has no
  attachment channel, so an enrolled caption row would re-emit its caption
  as plain text without its bytes; the eligibility gate refuses the
  descriptor-carrying family at enrollment, a pre-batch enrolled row is
  refused at claim and rebuild with the deterministic failure settled, and
  the manual retry face refuses it too), the §4.135 pull-path
  `conversation.changed` events (the `contacts.sync`/startup pull now
  mirrors the §4.122 passive batch's event semantics — the store returns
  the skeleton change set and each newly materialized or title-upgraded
  conversation rides `conversation.changed`, so a desktop already rendering
  the list refreshes after an engine restart or a fresh link without a
  refetch), and the §4.136 `syncMessage.blocked` consume arm (the engine
  already projects the official `_handleBlocked` mirror modern+legacy; the
  connector consumes it into the §4.131 blocked factor store and rides
  `conversation.changed`, tolerant parse, presence-stamps-true only). No
  new method, event, key, error, capability, or store move. Tests 765 →
  771 (lib 583 → 589), all green) —
  previously 1.139 (2026-10-13; the batch-C emit surface completes:
  the live contact-card branch (§4.132 — the engine's live
  `dataMessage.contact` / `syncMessage.sentMessage.contact` faces parse
  through the §4.38 import authority's exact bounds, the wrapped avatar
  pointer `{avatar, isProfile}` unwrapping onto the descriptor list,
  `isProfile` — the engine's address-book fact — never riding, the
  bodyless card a real incoming/outgoing row in the history chain's wire
  shape, a card beside a control echo keeping its control routing, and
  the queue budget counting the card's heap strings entry-wise; the
  whole-card discipline — no usable content, no face, plain routing —
  carries over untouched). Wire additive-only: live rows begin carrying
  the `rich.contact` key the import rows already carried, no changed
  shape, error, or capability move. Tests 763 → 765 (lib 581 → 583),
  all green) —
  previously 1.138 (2026-10-13; the batch-C list surface
  (§4.128–§4.131; the sections carry the full records): the typing
  projection carries the sender identity (§4.128 — `conversation.typing`
  additively carries `senderId`/`senderName`, absent-key), the
  conversation summary carries its protocol identifiers (§4.129 —
  `serviceId`/`e164`/`groupId` derived from the peer-key shape, `pni`
  reserved-never-present), the draft stamp rides the summaries with two
  new local faces (§4.130 — `conversations.saveDraft`/
  `conversations.clearDraft`, timestamp-only, draft text stays
  desktop-side safeStorage), and the blocked mask reaches the list and
  the event stream (§4.131 — the summary's always-serialized `blocked`
  bit, written by the §4.47 write-back mirror, the §4.58 BLOCK-family
  verbs, and the enrich's own isBlocked; one writer per key family).
  Wire additive-only: two new methods, two optional event keys, four
  absent-key + one always-serialized summary keys, one new store table,
  no changed shape, error, or capability move. Tests 755 → 763 (lib
  573 → 581), all green) —
  previously 1.137 (2026-10-13; the batch-C connector emit surface
  begins (§4.125 overview; the sections carry the full records): the
  outbound link-preview `date` send face (§4.126 — `messages.sendText`
  preview entries gain the optional `date` stamp, 0 reads as absent, the
  value rides upstream as `previewDate`, the pending row's rich persists
  the card and the retry rebuild restores it, image bytes stay
  transit-only), the outbound attachment blur-placeholder face (§4.127 —
  singular and plural `messages.attachments.send` payloads gain the
  optional `blurHash`; the pending row's descriptor carries it from the
  first tick; the engine's data-URI attachment channel has no blurHash
  key — the upstream gap is documented, not invented), and the
  §4.133 admin-delete-window verification record (the official window is
  a client-side remote-config gate, the protocol wire carries none — the
  connector's no-precheck posture is already the honest face; no
  implementation). §4.128–§4.132 (the typing summary projection, the
  conversation-summary identifiers, the draft timestamps, the blocked
  mask, the live contact-card branch) land in the same batch's
  subsequent commits. Wire additive-only: two optional request keys,
  no new method, event, error, capability, or store move. Tests 753 →
  755 (lib 571 → 573), all green) —
  previously 1.136 (2026-10-13; the identity pending pass-through
  (§4.120): the `identities.get` projection additively carries the KT
  engine's optional boolean `pending` — the engine sidecar key marking a
  peer identity key that was replaced without confirmation, the same
  single source the engine's send gate reads (the "Identity unverified"
  send-error family). The connector forwards the key verbatim when the
  engine answered it and omits the key when an older engine answer has
  none — the same optional-key pass-through the pinned JsonIdentity
  nullable trio practices; the connector invents neither value nor
  grammar, and a non-boolean engine `pending` reads as absent (the honest
  no-signal face). Desktop consumption is presentation-only: the
  safety-change banner judgement widens to
  `trustLevel === 'UNTRUSTED' || pending === true`, closing the
  first-contact gap — a never-verified peer whose key was replaced before
  any trust entry projects TRUSTED_UNVERIFIED (the three-value vocabulary
  carries no signal) and used to miss the banner; the additive key is the
  only honest signal. No new method, error, capability, or store move;
  read lane unchanged; additive wire only. Tests 733 → 734 (lib 554 →
  555), all green) —
  previously 1.135 (2026-10-12; the live poll receive chain lands
  (§4.118): the engine's six-question-batch Q3 poll receive faces —
  `dataMessage.poll {question, options, allowMultiple}` /
  `pollVote {targetAuthor, targetSentTimestamp, optionIndexes, voteCount}` /
  `pollTerminate {targetSentTimestamp}` (engine jsonrpc-compat Q3 收投影,
  official `processDataMessage.preload.ts:395-441` behavior anchor) —
  reach the receive chain. The bodyless poll create is a real message row
  landing the exact `NormalizedPoll` the §4.38 import chain lands (one
  struct, one serde face — the wire `poll` key byte-identical, the
  desktop's history-shape consumer unchanged); the vote and the
  termination are control receives that fold onto the row they address
  (never a row of their own) and re-emit it on `message.upserted` (the
  edit arm's event shape). Row addressing is the (sentAt, author-pair)
  §4.6 identity with three author candidates — the account's own
  outgoing mirror, the anchor's sender hash, and the §4.37
  `peer_identities` reconciliation for an ACI-shaped anchor — failing
  closed on absent/ambiguous/non-poll rows; an early fold buffers
  §4.91-style per candidate and converges on the create's landing.
  Vote semantics are the official replace-on-vote (the voter's full
  selection replaces in place, an empty selection clears, out-of-range
  indexes never render and never erase), the voter being the envelope
  source or the account itself on the own echo (the 自发回声归属: the
  multi-device echo lands the outgoing mirror row, never an unread,
  never someone else's event); `voteCount` never transports (the §4.38
  projection gap verbatim). The termination anchors the envelope sender
  = the poll author (the official `onPollTerminate` posture) and is
  idempotent. The dedupe is the import chain's own: a history refresh
  after a live landing skips at `insert_history_batch`'s number/ACI
  duality identity — no duplicate row, and the shared row-level skip
  boundary (archived votes do not merge into a live-landed row) rides
  the same discipline every face already rides. The 1.81-era
  import-only comments on the poll face are repaired to describe both
  chains. Wire additive-only: zero new keys (the face has ridden rows
  since 1.81), no new method, error, or capability, no schema JSON
  change, zero send-side coupling (the §4.106 send faces untouched).
  Tests 717 → 729 (lib 539 → 550), all green) —
  previously 1.134 (2026-10-12; the §4.114 front-run batch lands
  (§4.117): the engine's four account-scoped stream notifications the
  connector used to drop — `receive.emptied`, `receive.clockskew`,
  `groupTerminated`, `contactProfileUpdated` — joined by the fifth the
  engine shipped meanwhile (`receive.conversationState`, engine
  `e60d673`), all reach the host as transient events. Two lanes: the two
  envelope keys join the `control_routing` ladder as ControlReceive
  variants and forward through the conversation-less transient family
  (the terminated group forwards even when its row is gone), and the
  three standalone methods get the new account-domain transient-event
  lane — attributed EngineEvent variants whose signal account resolves
  through the store to the opaque accountId (the accountUnbound
  discipline: malformed stays a parse warning, unmatchable drops
  warn-only with a perf counter, never silently claimed as forwarded).
  `receive.conversationState` rides the same lane with the engine's
  conversations array forwarded verbatim — no entry parsing, no trim,
  the owner red line: a passthrough structure error corrupts every
  downstream consumer; the engine's own caps (book 8192/account, batch
  256 + overflow tail) govern the wire. Five new host event names
  (`receive.emptied`, `receive.clockSkew`, `group.terminated`,
  `contact.profileUpdated`, `conversation.stateChanged` — the naming
  record in §4.117), schema `$defs/event` grows the five members with
  their data branches (the conversation entries deliberately open
  objects); no new method, error, capability, or store version; the
  perf word list grows exactly `account_transient_forwarded` /
  `_unattributed`. Tests 709 → 717 (lib 532 → 539), all green) —
  previously 1.133 (2026-10-12; the S2a measurement layer lands
  connector-side (§4.116): the `receive_persistence_loop` per-envelope
  ingest transaction — the registered largest-bottleneck candidate
  (30-envelope burst = 30 transactions) — is timed and counted per
  attempt (`receive_ingest` timing, `receive_ingest_ok`/`_err`
  outcomes, `receive_ingest_events` realized batch size), and the
  doctor's per-group engine face gains the additive `perfMetrics`
  probe: the engine's read-only `engine.perfMetrics` snapshot (the
  engine slice's closed 31-key vocabulary) projected through the
  connector's pinned allowlist — known keys pass verbatim, unknown
  keys drop and count into the face's `connectorDroppedKeys`, never an
  unbounded forward — deliberately not the verbatim passthrough the
  `version`/`protocolCapabilities` faces use. Measure-only, zero
  behavior change; no content, no keys, no envelopes enter the layer.
  The S2a "vendored batch-window three-phase" leftover is verified
  engine-vendor-side (`kt-signal-engine/vendor/presage`: window
  fill/decrypt/batch persist) — the connector has no presage and no
  batch window, its receive leg is per-envelope by construction — and
  stays an engine-side batch. Wire additive-only: one optional doctor
  key; no new method, error, or capability; schema unchanged (the
  doctor result has never been schema-pinned). Same-batch doc repairs:
  the Status chain now carries the 1.122-1.132 index, §2 and README
  state the kt-engine reality (ADR 0003 fallback posture), the ADR
  0002 collision resolved (media-ingest ADR renumbered 0004, the
  multiple-instances ADR keeps 0002), 1.108-era count brackets.
  Tests 700 → 709 (lib 523 → 532), all green) —
  previously 1.132 (2026-10-12; the plaintext chat export reaches the
  host, §4.115 — the section carries the full record) —
  previously 1.131-1.122 (2026-10-12; each revision's record lives in
  its own section: the storage merge/import + call-link faces §4.112,
  the story distribution lists §4.111, the notification profiles
  §4.110, the serverTimestamp ingest §4.109, the group-call era update
  §4.108, the group create §4.107, the polls §4.106, the contact card
  §4.105, the GV1→GV2 migration §4.104, the device plane §4.103) —
  previously 1.121 (2026-10-12; the media-ingest gate flips to
  pass-through (§4.102): the registry's `effective_media_ingest` — the
  kt-engine era's forced downgrade that kept `--ignore-attachments` on
  the argv and answered every media method CAPABILITY_UNAVAILABLE
  however the launcher decided — now passes the launcher's
  `--media-ingest` decision through verbatim. Main-path policy flip,
  not a new execution surface: every media face, the governor, and the
  1.98 advertisement gate are unchanged; what changes is the decision's
  owner. The §5 launch bullet and the §6.7 media-limitation red line
  are struck through in place with pointers; invariants held —
  locator-less legacy rows stay permanently non-downloadable
  (INVALID_REQUEST before any network, honest copy), pointer material
  never logs, queues stay bounded. No new method/error/tag; no store
  move. Tests 663 held (lib 505 held; one unit flips in place), all
  green) —
  previously 1.120 (2026-10-12; the account-settings write face
  gains the linkPreviews bit (§4.101): the engine's 窗D B6 批1 landed
  `setAccountConfiguration`'s optional linkPreviews write (the official
  AccountRecord field 11) plus the five-boolean getConfiguration read
  projection; the connector's §4.45 `accounts.updateConfiguration` was
  keyed to the four-key shape, so the desktop could not write the
  link-preview auto-generation bit at all. The optional boolean joins the
  at-least-one ladder (a lone linkPreviews is a legal write), forwards
  only when provided (the partial-write semantics unchanged), and shares
  one wire key with the read side — the §4.87
  `settings.configurationSync` projection has carried `linkPreviews`
  since 1.106, so the 1.120 linkage test pins write-face key and
  read-mirror key as the same name end to end. No new method, error, or
  capability; no store shape. Tests 663 held (lib 505 held; the two
  extended tests grow assertions in place), all green) —
  previously 1.119 (2026-10-12; the blocked read carries the
  block stamp (§4.100): the engine's 窗D B7 read-projection
  `blockedAtTimestampMs` rides the §4.45 `conversations.getBlocked`
  entries additively — present only when the record holds a positive
  stamp, 0-is-unknown stays absent (the deletedAtTimestampMs family
  posture), GV1 records never carry the key. Per-key shape-tolerant
  parse; a pre-B7 answer reads byte-identically. Record-face additive
  only: no new method/error/tag, no store move. Tests 663 held (lib 505
  held; the two extended tests grow assertions in place), all green) —
  previously 1.118 (2026-10-12; the delete-for-me upstream leg
  rides the engine sync face (§4.99): `messages.deleteForMe` behind the
  new `delete-for-me` capability tag closes the §4.83 family's outbound
  half — the engine `deleteForMeConversation` face, the official
  deleteConversation sync. The §4.97 addressing trio resolves through the
  shared ladder; `localOnly` absent/false is the full-clear arm
  (mostRecentMessages 1..=5 {author, timestamp} entries verbatim — the
  official slice(0, 5) mirror, oversized refused fail-closed, the
  optional non-expiring companion ≤5), `localOnly: true` is the
  local-only arm (no entries allowed), legacyGroup refuses
  deterministically. Receipt-class, the §4.89 discipline verbatim: any
  engine call failure degrades {status: sent|unknown} with no error
  object and is never retried. Zero local writes — the §4.83 receive
  mirror stays the inbound channel, this face is the outbound leg only.
  No new error codes; no store shape moves. Tests 661 → 663 (lib 504 →
  505), all green) —
  previously 1.117 (2026-10-12; the group invite-link family rides
  the engine link faces (§4.98): six methods behind the new
  `group-invite-links` capability tag. The account-less read faces ride
  the default group's engine (the §4.32 anonymous-face discipline):
  `groups.generateInviteLinkPassword` (empty params → a fresh 16-byte
  password, standard base64) and `groups.parseInviteLink` (link →
  {groupId, password}, the three official URL forms plus the bare code).
  `groups.buildInviteLink` resolves the group row through the shared
  ladder and takes the optional password — explicit = pure construction;
  absent = the engine reads the server group state's link password, an
  unset link answering GROUP_INVITE_LINK_UNAVAILABLE (retryable=false,
  the honest not-set state). `groups.previewViaLink` answers the
  official pre-join projection; `groups.joinViaLink` answers
  {status: joined|requested} and `groups.cancelJoinRequest` answers
  {status: cancelled} — both send-lane Mutating with the
  SEND_OUTCOME_UNKNOWN no-retry discipline and the record-before-parse
  dispatch log. Password and link are single-pass material forwarded
  verbatim — never shape-gated, logged, or persisted connector-side; a
  malformed link folds into UPSTREAM_ERROR retryable=false. New
  allowlist codes GROUP_INVITE_LINK_UNAVAILABLE and
  GROUP_INVITE_LINK_PREVIEW_FAILED (retryable=false, data.status?
  verbatim); the join/cancel server-rejection channel rides the existing
  GROUP_UPDATE_FAILED code. Tests 657 → 661 (lib 501 → 504), all green) —
  previously 1.116 (2026-10-12; the conversation-state trio rides
  the storage face (§4.97): `conversations.getState` /
  `conversations.setState` carry the engine's
  getConversationState/setConversationState faces — the official Storage
  Service record's mute/archive/marked-unread bits — behind the §4.47
  blocked face's addressing trio (accountId/conversationId/kind) and the
  shared resolution ladder. The write is partial by grammar: only the
  flags the caller sent ride the payload, an empty intent answers
  INVALID_REQUEST locally, and the answer is the post-write projection
  read back; both faces persist nothing locally. No new error codes: the
  engine's canonicalization refusals arrive CONVERSATION_NOT_RESOLVED
  (retryable=false), the outage family passes through as STORAGE_*
  (retryable), and the mutating face keeps the reaction-class
  SEND_OUTCOME_UNKNOWN discipline. Advertised through the new
  `conversation-state` capability tag. Tests 654 → 657 (lib 499 → 501),
  all green. Counting recalibration: this revision's total is the summed
  cargo-test `passed` across every target, re-measured at the 1.115 tag
  (654, not the 658 previously recorded — the integration legs were
  over-counted by four in the earlier bookkeeping; the per-batch deltas
  and the lib counts were exact throughout, and from here the base is
  the reproducible cargo-test sum)) —
  previously 1.115 (2026-10-12; groups.update / groups.quit ride
  the engine write face (§4.96): the engine batch landed the full
  `updateGroup` write family (title + avatar tri-state + member arrays +
  the invite-link access word) and the quit write under its own method
  name `leaveGroup`. The connector folds its validated change set onto
  the engine payload (groupName→title, members verbatim, avatar verbatim,
  linkState→addFromInviteLinkAccess ANY/ADMINISTRATOR/UNSATISFIABLE);
  any change key outside that subset answers CAPABILITY_UNAVAILABLE
  naming the keys — the honest whole-call refusal, never a partial
  application. The engine's deny faces are deterministic: both call
  sites map `EngineError::Upstream` to UPSTREAM_ERROR with
  retryable=false. The server-rejection family gets its own allowlist
  code `GROUP_UPDATE_FAILED` with the engine's `data.status?` verbatim
  (retryable=false — a 409 revision race unblocks only after re-reading
  the group state, never by blind replay). Schema: the error enum and
  the two method descriptions restate the wired face. Tests 657 → 658
  (lib 498 → 499), all green) —
  previously 1.114 (2026-10-11; the identity gate gets a code
  (§4.95): the engine's send path refuses a send when the recipient's
  identity key changed without confirmation — official desktop's
  safety-number gate — and folds the refusal into the plain `-32603` face
  whose message embeds the fixed `(UntrustedIdentityException)` anchor.
  The §4.80 send-chain classifier knew three anchors, so a safety-number
  refusal answered the generic `UPSTREAM_ERROR` face: indistinguishable from
  noise, retryable=true, with no way for the desktop to say what unblocks
  the send. The classifier gains the fourth anchor and a content-free
  `IdentityUnverified` variant; the service mapping answers
  `SEND_IDENTITY_UNVERIFIED` with retryable=false (confirming the safety
  number is the only unblock); the schema error-code enum gains the code;
  the `ENGINE_STRUCTURED_ERROR_CODES` allowlist is deliberately untouched —
  the code travels the anchor-classification face, not the structured-code
  face. Tests 656 → 657 (lib 497 → 498), all green) —
  previously 1.113 (2026-10-11; the accountUnbound whitelist (§4.94):
  the engine's unbound stop-ring (B4) already pushed a single `accountUnbound`
  notification when the server's 401 credential rejection halted the receive
  loop — wire-additive, deliberately unconsumed. That left the account's death
  reachable only through pull-shaped evidence: the host lane learned of the
  dead link only when some later send failed with the authorization signature,
  and until then the desktop kept rendering a live account whose receive loop
  was parked for good. The dispatch now carries an arm for the notification: a
  strictly-parsed non-empty `params.account` replays onto the
  `AccountUnauthorized` lane the account-unlink watcher already consumes
  (mark → `account.changed`), and a malformed payload — missing, non-string or
  empty account — cannot be attributed and degrades to the
  unknown-notification warning instead of inventing an account. No host or
  schema change; no wire change (the notification is engine→connector only).
  Tests 654 → 656 (lib 495 → 497), all green) —
  previously 1.112 (2026-10-11; the ending never regresses (§4.93): the live
  message insert and the send completion still wrote the conversation/account endings
  unconditionally, and both can carry an older timestamp than the summaries already
  publish (per-sender clocks make delayed deliveries real; the completion re-stamps with
  the engine-assigned upstream timestamp that can sit below newer rows and the pre-send
  local clock). Both writers now take the max in place — the CASE guard the
  history-import backfill already ran, extended to the live paths it was protecting —
  with the insert-path preview moving together with its timestamp and the unread counters
  deliberately unconditional (an old message still deserves its badge; the 1.29
  same-transaction bump untouched). No wire change. Tests 653 → 654 (lib 494 → 495),
  all green) —
  previously 1.111 (2026-10-11; the startup sweep (§4.92): a 'pending' outgoing
  row is a send whose completion never landed — the process died between the insert and
  the engine answer — and nothing can ever complete it again, because the only writers of
  that transition lived in the process that died; the desktop saw a spinner that outlived
  its process, in the one state with no retry affordance. `Store::open` now runs one
  point UPDATE after the migrations, before any runtime/host/CLI consumer:
  pending → failed. The honesty is ownership, not time — one instance owns one store (the
  ADR 0002 kernel lock), so an orphaned pending row's completion can never arrive after
  open; failed is both the truthful state and the useful one (the desktop's retry action
  and the guarded rearm apply). Idempotent by construction: a re-armed row orphaned by
  another crash sweeps back on the next open. 'unknown' keeps its distinct contract
  state; no wire change. Tests 652 → 653 (lib 493 → 494), all green) —
  previously 1.110 (2026-10-11; the early-arrival edit/remoteDelete buffer (§4.91):
  the control plane's never-create discipline gave an edit or remoteDelete whose original
  row had not landed yet exactly one fate — a silent drop — and the engine's cold-start
  receive race makes that a real convergence loss (the row lands later with yesterday's
  body, or stands after its author deleted it). A bounded in-memory table now admits
  early RemoteDelete and body-carrying Edit control envelopes (512 entries, 5-minute TTL,
  oldest-eviction + lazy sweeps — the typing limiter's discipline at buffer scale; every
  other family and every conversation-resolving receive keeps the live arms
  byte-identical). Entries key to the identity the original will carry — (account, kind,
  peer_key, target_sent_at, author) — so the flush only ever applies to the author's own
  row (the §4.90 store scoping rides under it), and the drain runs at the landing insert
  with land-then-converge event order, applying the live arms' store paths verbatim so a
  converged row is byte-identical whether its envelope arrived early or late. The buffer
  rides the §4.90 age gate; expired entries leave silently. No wire change. Tests 648 →
  652 (lib 489 → 493), all green) —
  previously 1.109 (2026-10-11; the remoteDelete author and age gates (§4.90):
  the inbound remoteDelete control applied without checking who was speaking or how old
  the target was — the store's delete matched (account, conversation, sent_at) alone, so
  in a group author A's delete wiped author B's row that shared the per-sender timestamp,
  and a delete naming a target older than the official 24 h compose window applied anyway.
  The store delete is now author-scoped (`mark_remote_deleted` carries the envelope's own
  author projection — the row's `sender_id` derivation verbatim; direct-conversation
  identity pairs collapse to one string, so direct behavior is byte-identical; the §4.83
  delete-for-me sync keeps its deliberately author-less path), and the arm drops deletes
  whose sender-clock compose age — envelope timestamp minus target timestamp, immune to
  sender/receiver clock skew — blew past `REMOTE_DELETE_MAX_AGE_MS` (24 h), before the
  store call and before the §4.91 early-arrival buffer. No wire change. Tests 645 → 648
  (lib 486 → 489), all green) —
  previously 1.108 (2026-10-11; the read/viewed SELF-SYNC upstream legs (§4.89):
  `messages.syncRead` / `messages.syncViewed` close the connector's silent second
  read-state channel — the markRead pair spent its fan-out on the peer-facing
  `sendReadReceipt`/`sendViewedReceipt` faces (gated by the account's `readReceipts`
  flag, default off), so the account's own other linked devices never converged the
  desktop's read state; the engine's `markRead`/`markViewed` SyncMessage faces (the
  official syncReadMessages/syncView isomorphs) sat unconsumed. The two new methods
  carry byte-identical addressing, selection bounds, and receipt-class
  `{status: sent|unknown}` degradation to the markRead pair — one wire semantic per
  method so the desktop picks the channel by name (the peer receipt leg stays
  flag-gated, untouched) — and the supervisor spends the group on one engine
  `markRead`/`markViewed` call (`{account, reads|viewed: [{sender, timestamp}...]}`, the
  engine chunking 100/envelope); syncViewed keeps the §4.77 local-first `viewed_at`
  stamp, syncRead stays stamp-free. Schema additive: two enum entries, two if/then
  pairs, two `$defs`. Desktop call-site migration is a separate batch. Tests 640 → 641
  (a pre-recalibration total: measured under the bookkeeping the 1.116 entry below
  corrects — the integration legs were over-counted by four; the reproducible base
  re-measured at 1.115 is 654, and the all-targets baseline has since grown past it),
  all green) —
  previously 1.107 (2026-10-11; the ingest key-list completion (§4.88): the connector's
  ingest faces now consume the engine's P14 live-projection batch key-for-key — the faces the
  engine's live receive projects and the connector was silently dropping. (a) The attachment
  descriptor gains the render `caption` (text-budget truncated, empty = absent), the blur
  preview `blurHash` (non-empty only, 128-char bound), the streaming-integrity pair
  `incrementalMac` + `chunkSize` (both or neither — the official filePointers.preload.ts:84-87
  discipline; half a pair verifies nothing and drops whole) and the always-boolean
  `isBorderless` / `isGif` projections beside `isVoiceNote` (no flag bit = false, the
  three-boolean one-shape consumption form; the official isGIF contentType gate stays the
  desktop's judgement). (b) The link-preview card gains its `date` publish stamp (positive
  only — the official falsy posture). (c) The rich payload gains `dataMessage.expireTimerVersion`
  (positive, u32-bounded; the timer value itself stays the row's `expireSeconds` face) and the
  sent mirror's three transcript faces — `isRecipientUpdate` (true only), `unidentifiedStatus[]`
  (the destinationUuid PNI-prefixed/UUID form, `unidentified`, the `destinationPniIdentityKey`
  base64 within the identity-key bound) and `storyMessageRecipients[]` (`destinationUuid`,
  `distributionListIds` verbatim including the empty-list narrowing semantics,
  `isAllowedToReply` only when the proto stated one) — receive dataMessages never carry the
  keys, so inbound rows keep their defaults; the outer lists take the sync receive family's
  256-entry cap. (d) The archive import face parses the same-named keys and the same flag bits
  (Borderless=2 / Gif=8), so imported and live descriptors share one wire form. (e) The
  engine's typing `groupId` projection was verified already consumed since contract 1.15 (the
  routing group leg resolves group typing onto its conversation) — verified, zero delta. All
  parses are per-key fail-closed at the engine's own projection shapes; every pre-1.107 row
  reads byte-identical (the optional keys deserialize through `serde(default)`; the boolean
  trio serializes explicit false — the isVoiceNote shape); a bodyless, attachment-less
  dataMessage carrying only preview/P14 material keeps the bodyless drop discipline (键位扩齐
  不改行落位). No new method, event, error, or capability; the schema gains one additive
  description paragraph on the read shape; tests 635 → 640, all green) —
  previously 1.106 (2026-10-11; the sync seven-arm consume batch (§4.87): the engine's
  P14 group-5 sync projections — `configuration` / `fetchLatest` / `callLinkUpdate` /
  `callLogEvent` / `deviceNameChange` / `usernameChange` / `pniChangeNumber` — previously all
  falling to the plain `syncMessage` skip, now each consumed as one control routing plus one
  host event, the §4.84/§4.85 paradigm arm-for-arm. (a) The receives: every arm is fail-closed
  at its own facts — the configuration flags read per-key (a hostile shape answers absent; a
  face with no surviving flag skips), the fetchLatest/usernameChange arms route on presence
  alone (the official fires the trigger even type-less; the word keys ride the engine's
  SCREAMING enum whitelists per-key), the call-link update hard-gates exactly where the
  official handler throws (type must be the projection's only known member UPDATE, the
  rootKey must be present in bound — an over-bound key drops the record whole, never
  truncated) with the admin key per-key, the call-log mutation hard-gates its verb word
  (CLEAR / MARKED_AS_READ / MARKED_AS_READ_IN_CONVERSATION / CLEAR_IN_CONVERSATION — the
  record's whole meaning) with the rest per-key, the device-name change hard-gates the
  deviceId in u32 range (the official's own absent-key drop), and the PNI change forwards
  only the engine's stripped `{registrationId?, newE164?}` face (the key material never
  crosses the engine boundary; a zero-content face skips). (b) The forwards: seven new host
  events — `settings.configurationSync`, `sync.fetchLatest`, `call.linkUpdate`,
  `call.logEvent` (the raw identity riding `peerId`, the §4.85-ladder hit riding
  `conversationId`), `account.deviceNameChanged`, `account.usernameChanged`,
  `identity.pniChanged` — all flat, absent optionals staying absent (the story.received
  conditional-insert style). Store adjudications, per arm recorded in §4.87: no mirror
  anywhere — the account-level arms are trigger semantics with nothing to mirror
  (configuration/fetchLatest/deviceName/username/pni), the data-plane arms keep no store
  the official consumer does not already own (the connector holds no call-history store —
  the §4.85 discretion verbatim — and no call-link store, the §4.73 passthrough
  discipline); every record is transient, a missed delivery losing exactly what an official
  client loses offline. Desktop consumption stays out of this batch. No new method, error,
  or capability; the schema gains seven event enum entries, seven allOf data blocks, and
  four schema-pairing bounds (the call-link keys' 128 ↔ `MAX_SYNC_CALL_LINK_KEY_CHARS`, the
  device/PNI registration ids' u32 ceiling ↔ the parse's `try_from` range gate); tests
  624 → 635, all green) —
  previously 1.105 (2026-10-11; the edit-send migration (§4.86): `messages.edit`'s
  upstream face moved off the signal-cli-era `send` + `editTimestamp` dispatch onto the
  engine's native `editMessage` wire method (the 2026-10-11 six-questions batch, official
  edit send face `Content.editMessage{targetSentTimestamp, dataMessage}`). The old call was
  the audit's only ❌-class send-path break: the kt-engine `send` carries no `editTimestamp`
  parameter, so every desktop edit went upstream as a brand-new message with the previous
  body discarded by the engine's parameter reader — the remote saw a duplicate message and
  the original stayed unedited. The host wire face is unchanged (`messages.edit`, the same
  params, the same `MessageRecord` result, the same `SEND_OUTCOME_UNKNOWN` indeterminate
  discipline, the same message-addressed row resolution): `prepare_edit` now emits
  `targetTimestamp: <sent row's protocol timestamp>` (the engine's u64 key, the same key
  name `remoteDelete` already dispatches on) and the supervisor dispatches `editMessage`
  with the same exactly-one-leg addressing (`recipient` array for direct, the 32-byte group
  master key `groupId` for groups — the shape `set_upstream_target` already produced, the
  `sendMessageRequestResponse` 同构 the engine's dispatch parses). Zero desktop changes. No
  new host method, error, or capability; the schema's `messages.edit` description is the
  only wire-surface text moved (no shape change, additive-only in the revision sense);
  tests unchanged green) —
  previously 1.104 (2026-10-11; the call-history sync consume face (§4.85): the
  engine's `syncMessage.callEvent` projection — the account's own linked device wrote one
  1:1/group call's history entry, the official callEventSync face — consumed as pure transient
  forwarding. (a) The receive: fail-closed shape gates on the record's primary key — the
  callId must be a non-empty all-ASCII-digit string at the u64 ceiling (the engine projects
  `u64::to_string()`, the official `longToString` form) and the timestamp strictly positive
  (the engine's zero-defaulted records are not history records); either gate failing drops the
  whole face and the envelope falls back to the plain `syncMessage` skip. The type/direction/
  event word keys fail per-key (each must be exactly one of the engine's lowercase projection
  words — a non-member answers absent, never a mangled leg). (b) The forward: the new
  `call.historyEvent` host event carries `{accountId, callId, timestamp, type?, direction?,
  event?, conversationId?}` — flat, absent optionals staying absent, the conversation identity
  resolved through the §4.85 ladder (the 32-byte group form addressing its group row directly,
  the UUID/e164 forms through the §4.81 resolution family; the ad-hoc hex room id honestly
  resolving nothing). No store mirror, by recorded discretion: the connector keeps no
  call-history store and inventing one would fork the desktop's authoritative call log (the
  official consumer writes its own history through `updateCallHistoryFromRemoteEvent`); the
  record is transient — a missed delivery loses that one entry exactly as the official client
  loses one it received while offline. No new method, error, or capability (the
  `message.expired` precedent for core events); the schema gains the event enum entry, the
  allOf[13] data block, and two schema-pairing bounds (the callId's 20 ↔
  `MAX_SYNC_CALL_ID_CHARS`, the conversation identity's 128 ↔ `MAX_RECEIVE_ID_CHARS`); tests
  621 → 624, all green) —
  previously 1.103 (2026-10-11; the verified trust-sync consume face (§4.84): the
  engine's `syncMessage.verified` projection — the account's own linked device settled one
  peer's safety-number trust state, the official SyncMessage.verified face — consumed through
  two faces. (a) The receive: a fail-closed parse whose gates are the engine's lowercase trust
  word whitelist (`verified`/`unverified`/`default` — anything else, including the wire enum
  numbers, is a hostile face) and the ServiceId UUID shape gate on the destination (the engine
  projects `service_id_string()` verbatim); the optional `destinationNumber`/`identityKey` keys
  fail per-key (a hostile shape answers absent — a fingerprint is never truncated into a lie),
  and a face that fails the hard gates falls back to the plain `syncMessage` skip instead of
  becoming a phantom event. (b) The forward: the new `identity.verifiedChanged` host event
  carries `{accountId, destinationUuid, state, destinationNumber?, identityKey?,
  conversationId?}` — flat, absent optionals staying absent (the story.received
  conditional-insert style), with the destination laddered through the §4.81 resolution family
  to the local `conversationId` when it resolves. No store mirror, by recorded discretion: the
  engine already applied the state to its authoritative sidecar store before projecting
  (`identity::apply_synced_trust` upstream of the projection), the connector store carries no
  trust bit to mirror into (conversations/contacts/peer_identities are all trust-less), and the
  host's reconciliation path is the §4.61 `identities.get` read-through — a second authority
  would only drift. No new method, error, or capability (the `message.expired` precedent for
  core events); the schema gains the event enum entry, the allOf[12] data block, and two
  schema-pairing bounds (the destination's 128 ↔ `MAX_RECEIVE_ID_CHARS`, the key's 128 ↔
  `MAX_SYNC_IDENTITY_KEY_CHARS`); tests 618 → 621, all green) —
  previously 1.102 (2026-10-11; the delete-for-me consume face (§4.83): the
  (§4.82): the layer the 2026-10-09 real-device debugging chain (CSS → DNS/socks5h →
  proxy split → the credential 401) exposed — a dead linked-device credential reached
  the desktop as the generic retryable `UPSTREAM_ERROR` family ("send failed, please
  retry") because its evidence never survived to a face the connector's matchers
  classify. Two faces, one independent state: (a) the classifier — the chat service's
  plain-text 401 body (`Credentials are required to access this resource.`; never a
  JSON error object, invisible to the exception-name matchers) classifies at the §4.17
  observation point through a standalone pure matcher beside the frozen match set (the
  §4.80 boundary note verbatim), folding into the same `EngineError::Unauthorized` arm
  — `ACCOUNT_UNLINKED` stays the host face and the store's `device_unlinked` state
  stays the credential-invalid answer distinct from the retryable network families; no
  new error code; the match is one exact case-sensitive sentence, so the §4.17
  signatures, the §4.80 transport anchors, structured codes, and ordinary errors never
  trip it. (b) The projection — `runtime.doctor` groups gain the `credentials` face
  (§4.79 amended, `docs/doctor-contract.md` updated), the doctor's one
  account-authenticated probe: one read-only `getUserStatus` ride through the group's
  own live engine for the watchdog's ping-target account under the same 3-second
  diagnostic budget, folding into exactly one fixed literal — `healthy` /
  `credentialsInvalid` / `indeterminate` / `unreachable` / `noAccount` — with the
  probe target never entering the report. The engine-side residual is recorded, not
  fixed here: the vendored websocket-upgrade path drops the 401 status
  (reqwest-websocket handshake failure → `ServiceError::WsError`), so upgrade-layer
  credential death still answers the content-free `Internal error` face and the doctor
  answers `indeterminate` on it — honest until the engine batch lands the
  upgrade-status classification (the engine repository stayed read-only during this
  batch). No schema change (the 1.97 free-form-result precedent; method/error/event
  enums untouched); tests 609 → 614, all green) — previously 1.100 (2026-10-11; the engine D-batch consume faces (§4.81): four faces
  where the connector stops dropping engine projections and starts consuming them. D2 — the
  cross-device read/viewed sync (`syncMessage.read`/`.viewed` route as a bounded 256-entry
  control receive; per-face first stamps land on the rows, the unread badge converges through
  the existing session-coarse `clear_conversation_unread` ladder with
  `ConversationChanged`/`AccountChanged` in tow and per-row `message.statusChanged` events
  riding along; the badge convergence is deliberately coarser than the official per-row
  stamping — the recorded deviation). D3 — the group-change batch (`groupChanged` forwards
  verbatim under the 32-entry/64 KiB bounds, the guaranteed faces fail closed, the timer leg
  mirrors through the §4.41 path without landing a timer-notice row, and the new
  `group.changed` event carries the batch to the desktop). D4 — the quoted-attachment
  metadata (`quote.attachments` rides the three-key QuotedAttachment shape through the same
  bounded parser — the 8-entry cap, id-less thumbnails drop, the locator half never
  serializes; the §4.38 import face stays metadata-only this revision). D5 — the
  device-capability registration read (`accounts.deviceCapabilities`: the idempotent engine
  PUT with the host answer stripping the engine's bare `account` identity, the capability
  bits verbatim, every engine failure folding to `UPSTREAM_ERROR`, and the §4.17 chain
  gaining no matcher). The method surface grows by one (schema method enum + allOf branch +
  `$def` paired); tests 600 → 609, all green) — previously 1.99 (2026-10-11; the send-chain
  transport error classification batch
  (§4.80): the engine's send face folds transport-layer failures into the plain internal_error
  JSON-RPC face (`code: -32603`) whose `message` embeds the cause class, so every rate limit,
  proof challenge, and server outage reached the desktop as the generic retryable
  `UPSTREAM_ERROR` family. The connector now classifies the three fixed anchor literals the
  engine contract pins verbatim — `(RateLimitExceeded)` (the upstream 429/413 rate-limit
  family), `(ProofRequiredException)` (the 428 proof-challenge family), and
  `(ServerUnavailableException)` (the 5xx server-unavailable family) — at the same single
  observation point that reads the raw upstream error body (§4.17's authorization matcher
  keeps head-of-line precedence and its match set untouched, so the `AccountUnauthorized` →
  `ACCOUNT_UNLINKED` recovery chain cannot regress), mapping them onto the new host codes
  `SEND_RATE_LIMITED` / `SEND_PROOF_REQUIRED` / `SEND_SERVER_UNAVAILABLE` with
  `retryable: false / false / true` — the flag is display-side only; a mutating send is still
  never auto-retried, and an unknown send outcome keeps §6.4's never-resend posture. The
  classifier is a standalone pure function, deliberately not a widening of the fragile
  prefix/suffix authorization route; an error message carrying no anchor keeps the generic
  upstream answer byte-for-byte) — previously 1.98 (2026-10-11; the honest media-download advertisement + CDN SOCKS route
  batch (§4.38 step 3, §4.54): two truths the handshake and the transport now carry about the
  same face family. Advertisement: the handshake `attachment-download` capability is the one
  conditional member of the `capabilities` array — it rides only when the runtime's media ingest
  is genuinely armed (the registry's effective launcher decision, the kt-engine downgrade
  included, and the exact gate every media method answers: the media table present) — so a
  connector without it omits the tag and the desktop renders attachment pills metadata-only
  instead of calling a face that answers `CAPABILITY_UNAVAILABLE` for every download; an armed
  runtime advertises the pre-1.98 payload byte-for-byte (the method enum and dispatch table are
  unchanged, and the desktop's `mediaIngest=!app.isPackaged` packaged-default-off posture is a
  product decision this revision deliberately leaves alone). Transport: the §4.38 step-3 CDN
  download client — the one machine behind `history.attachmentDownload`, `attachments.download`,
  and the story byte faces — reads the launcher's `KT_SIGNAL_SOCKS_PROXY` process environment
  (the doctor's tolerant parse: canonical `host:port`, an explicit `socks5(h)://` prefix
  normalized; a malformed value reads as unconfigured everywhere else too) and rides the GET as
  `socks5h://` — remote DNS, the same mapping the engine child gets for `ALL_PROXY` — so the
  transit-CDN fetch shares the route the engine's own transport takes through walled or
  DNS-poisoned networks instead of a doomed direct dial to cdn*.signal.org; a missing value
  builds the previous direct client, and the connect 10 s / total 120 s / no-redirect posture
  is unchanged) — previously 1.97 (2026-10-10; the runtime.doctor Signal service-plane probe (§4.79): the
  report grows a top-level `service` face — `proxyConfigured` (the launcher's
  `KT_SIGNAL_SOCKS_PROXY` process environment present and well-formed; per-group proxy plans are
  deliberately not consulted, a flag-only proxy reads as unconfigured) plus `chat` and `storage`
  probes, each folding into exactly one fixed classification literal — `not_configured` (no proxy
  environment, no network attempt at all), `socks_refused` (the SOCKS-routed link refused at or
  below the proxy hop), `tls_failed` (the link reached the TLS handshake and failed against the
  vendored Signal production root CA — the only trust anchor, the kt-engine's tls_certs_only
  attitude), `timeout` (the 3-second connect/overall diagnostic budget), and `http` (the service
  edge answered; the only classification carrying `status` and `latencyMs`, with chat read
  healthy at its unauthenticated 401 and storage healthy at any HTTP response) — the probes are
  two read-only HTTPS GETs to fixed compile-time URLs through a SOCKS5H client built fresh per
  diagnostic (redirects off, no system/env proxy fallback), and the red-line discipline is
  unchanged and tightened: the report carries only the fixed literals plus a status code and a
  latency — never upstream error text, never request content, never a URL, never a proxy
  endpoint, and the doctor's "never probes the network" rule is now scoped to "never probes
  beyond the two declared service-plane faces") — previously 1.96 (2026-10-10; the runtime.doctor read-only diagnostic face (§4.79):
  one new Control-lane method answers the whole diagnostic payload in a single call — the
  version chain (the connector's own version/apiVersion/pid, and per group the engine's
  `version` and `engine.protocolCapabilities` answers riding through verbatim, an engine
  that did not answer honestly `present:false` with a static reason instead of an invented
  value), the data-dir face (path/exists/writable/freeBytes/totalBytes from an
  access+statvfs probe that writes nothing), the lock face (the
  `.kt-signal-connector.lock` occupancy classified none/held/stale/released — `held`
  carrying the kernel's liveness guarantee plus the acquire-time pid, probed by a kernel
  flock taken and dropped, never broken or removed), a bounded recent-error ring (the
  newest 64 host dispatch errors and engine protocol warnings aggregated by family,
  count-descending, in-memory and content-free), and the network face (the existing
  watchdog-restart and receive-drop counters read out as-is) — the discipline is read-only
  end to end: the doctor never starts or stops an engine, never writes a byte, never
  breaks a lock, the engine is probed only through its existing read face under a
  3-second budget, and an unreachable, exited, faulted, or mid-transition engine degrades
  its slice of the report honestly while the call still succeeds (params `{}` only —
  anything else answers INVALID_REQUEST retryable=false; no new error codes, no new
  capability token — the method rides the handshake method-list advertisement; the
  field-level consumer reference lives in `docs/doctor-contract.md`) — previously 1.95
  (2026-10-10; the chat-folder sync passthrough batch (§4.78): the
  desktop's folder picker gains its connector face — `chatFolders.list` (read lane, the new
  `chat-folders` capability) and `chatFolders.setRecords` (send lane, account-scoped
  mutating, the reaction class) forward the kt-engine's own Storage-record folder surface
  verbatim — `list` answering the manifest in the engine's storage order, `setRecords`
  answering the post-write manifest, so the caller never needs a second read. A record is
  the engine's own eleven-key JSON shape passed through untouched: the connector never
  re-derives folder syntax, never stores a byte of record content (zero new columns, zero
  new rows — the cloud record is the whole state), gates only the identity face (an object
  carrying a non-empty ≤128-char `id`, each half ≤64 entries, at least one half non-empty),
  caps the projection at 64 entries with `id`-less entries dropped, and the kt-engine-only
  seam answers JVM/native CAPABILITY_UNAVAILABLE after the local gates (stock signal-cli
  carries no `chatFolders.*` verb) — previously 1.94 (2026-10-10; the viewed-state
  projection + story-reaction batch
  (§4.77): the desktop's voice-note PlayedDot and story AnimatedEmoji surfaces gain their
  connector truth — `viewedAt` joins the message-row projection and the
  `message.statusChanged` event with the official `isVoiceMessagePlayed` semantics on both
  directions (incoming: the local `messages.markViewed` first-stamp; outgoing: the first
  peer VIEWED receipt's envelope timestamp, folded through the §4.70 split so a replayed
  receipt still emits nothing), and `stories.sendReaction` lands the pinned upstream's own
  `sendReaction --story` face behind the story anchor, kt-engine answering
  CAPABILITY_UNAVAILABLE until its engine batch arrives — previously 1.93 (2026-10-09; the
  adjacent visual-media probe batch (§4.76): the
  desktop lightbox's cross-message navigation gains its history face — `messages.adjacentMedia`
  (read lane, the `message-adjacent-media` capability) projects the official
  `getOlder/NewerMessagesByConversation` probe with `requireVisualMediaAttachments`
  (Signal-Desktop `ts/state/ducks/lightbox.preload.ts` `showLightboxForAdjacentMessage`,
  `ts/sql/Server.node.ts` `getAdjacentMessagesByConversation`) onto the connector store: an
  anchor message id the caller already holds resolves to its stored `sent_at` (a vanished
  anchor answers INVALID_REQUEST retryable=false, never silently re-anchored), a bounded
  keyset scan walks the probe direction until `limit` (1..=20) media-bearing rows accumulate
  or the range exhausts, and the strip predicate keeps exactly the desktop lightbox's own
  bar — at least one `image/*`/`video/*` attachment, view-once rows always excluded, story
  rows impossible by construction (a story never lands in the messages table, §4.67). The
  result is `{items: MessageRecord[]}` adjacency-first in the `messages.list` stored shape
  (reactions and edits attached the same way); an empty array is the honest no-more-media
  answer. Store-only read with deliberately no unread-clearing side effect — probing the
  lightbox's neighbors is not viewing the thread — previously 1.92 (2026-10-09; the
  group timer-change timeline row batch (§4.75): the
  2026-10-06 dated note's open presentation-policy boundary closes — a group disappearing-timer
  change (the engine's `groupUpdated {groupId, revision, expireTimerSeconds, editor?}`
  projection) now lands the official presentation face beside the mirror: the §4.41
  timer-notice system row in the group timeline (the §4.41 material carrying the announced
  value verbatim, the §4.41 rich marker, system direction/status, no expire columns — the
  notice row never sweeps). The row's sender face follows the §4.75 attribution ladder: the
  editor ACI resolves through the recorded ACI→peer-key mapping first (the §4.37 identity
  table — the desktop roster joins the number form), an unmapped ACI rides raw (joined by
  uuid), and an editor-less envelope keeps the conversation-scoped unattributed sender (the
  official unknown-member branch, never a guessed identity); `sender_id` carries the raw
  address un-hashed so the desktop's roster join and actor resolution work. A same-value event
  still lands its row (the official row narrates the change envelope and the group revision
  advanced); a replayed envelope (same identity tuple) dedupes through the row identity; a
  stamp-less envelope keeps the mirror and lands no row. Zero schema shape change (the row
  reuses the §4.41 row fields; the receive description sentence is corrected the same
  revision); no store version change. Recorded seam, untouched: the JVM/native dataMessage
  timer-notice path (signal-cli's native `isExpirationUpdate`) keeps landing its sender as
  the bounded legacy hash — joining that sender to the roster is the pre-existing §4.41 gap,
  not this batch's — previously 1.91 (2026-10-09; the story link-preview url/title metadata batch (§4.74):
  the §4.71 second-phase item "story text/preview-image dispatch" lands its metadata half —
  `messages.sendStory` grows two independently optional keys, `previewUrl` (present ⇒ non-empty,
  ≤2048 chars, `https://`-prefix — the official `isValidLink` https-only posture, unlike the
  §5.10 message link-preview face which admits both schemes) and `previewTitle` (present ⇒
  non-empty, ≤1024 UTF-8 bytes, the §5.10 title budget); every violation answers the
  deterministic INVALID_REQUEST retryable=false ahead of the account ladder and the mode seam —
  a violating value is never silently dropped and never forwarded; past the gates both ride the
  engine frame verbatim as the camelCase keys beside the byte-identical `previewImage` pointer
  key (the parallel engine batch composes the proto `Preview{url,title,image}` and the
  `project_story_message` `preview.url`/`preview.title` projection). Receive/verify half: the
  story receive chain was audited hop by hop and is a verbatim pipe with no key whitelist
  anywhere — engine envelope `storyMessage` → `bounded_story_material` (whole-object clone,
  serialized-size bound only) → `ControlReceive::Story` → the transient `story.received`
  event's `story` object — so the new sub-keys reach the desktop naturally; regression tests
  pin `preview.url`/`preview.title` (plus an unnamed key proving the no-whitelist posture) at
  the engine normalization, the service event, and the host wire frame, and the supervisor wire
  chain pins the camelCase keys on the full-option sendStory dispatch (lean frame stays
  key-free). No new method/event/error code/capability (`stories` widened in place, the 1.82
  no-new-bit precedent); no store version change; schema grows the two params properties
  (maxLength pairs keyed to the new constants, the url `pattern` pinning the https prefix) and
  the method description — previously 1.90 (2026-10-09; the setProfile avatar tri-state repair (§4.45): the
  explicit-null remove arm was unreachable on the real wire path — serde's `Option<Value>`
  folds a JSON null into absence at the option layer, so the service ladder's
  `Some(Value::Null)` arm was dead code and every explicit-null call silently kept the
  avatar (the 1.46 shape's recorded boundary, surfaced by the 1.89 batch's dedicated-type
  work). The face now shares the 1.89 tri-state deserializer type (renamed
  `GroupAvatarUpdate` → `AvatarUpdate`, one canonical type, two consumers): absent = keep
  (the `#[default]`), explicit null = remove (`avatar: null` forwards as a present-null
  key), data URI = replace — the validation ladder is unchanged (non-image family /
  non-canonical base64 / over-budget decode answer INVALID_REQUEST in the service before
  any upstream call; a non-string non-null payload now fails at params deserialization,
  the §4.45 discipline). Tests upgraded to pin the wire line: the integration null assert
  distinguishes a present-null key from a dropped key, a keep case pins the absent face,
  and the unit deserializer pin covers keep / remove / non-string at the type level. No
  engine-behavior change is implied for engines that never saw a real remove (the arm was
  dead); the schema's setProfile `avatar` widens to `["string","null"]` the same revision —
  previously 1.89 (2026-10-09; the groups.update group-avatar passthrough batch
  (§4.44): the `avatar` tri-state joins the params face — the `accounts.setProfile` shape
  verbatim (§4.45): absent = keep, explicit null = remove, a `data:image/*;base64,` data URI
  (canonical base64, decoded 1..=10 MiB — the engine's stated avatar budget) = replace; the
  Remove and Replace forms count as the change set's avatar member (a lone avatar call is a
  real change, not an empty set), the value rides the upstream payload verbatim (an absent
  avatar adds no key, an explicit null forwards `avatar: null` — the §4.44 omission
  discipline), and the connector never decodes image bytes beyond the budget check. The
  three states ride a dedicated deserialization type (`GroupAvatarUpdate`) because serde's
  `Option<Value>` — the setProfile face's shape — folds an explicit null into absence on the
  real wire path, a recorded boundary of the 1.46 shape that this face keeps reachable end
  to end. Honest
  degradation, the §4.68 snapshot-import shape: no bound engine serves the group-avatar byte
  face today — the JVM/native face takes a local avatar file path (the connector has no
  host-file upload channel, §6.7; no temporary file is ever written and no URI is forwarded
  into an upstream protocol error) and answers CAPABILITY_UNAVAILABLE (retryable=false)
  after the full local ladder, while the kt-engine face keeps its updateGroup-debt answer
  (the parallel engine batch owns the byte face and the gate removal — the payload side
  needs no further connector change when it lands). Error face:
  GROUP_AVATAR_UPLOAD_FAILED joins the structured-error allowlist verbatim — the
  PROFILE_UPLOAD_FAILED channel (§4.49) group-faced: the engine's partial-success data
  object {retryable, textSaved} rides verbatim with retryable=true (the group change landed,
  the avatar upload chain failed); the STORAGE_* family semantics are unchanged. No new
  capability (the face stays under `group-management`), no method/event/store change
  — previously 1.88 (2026-10-09; the call-link credential material
  passthrough batch (§4.73): the five `calls.*` engine material faces
  (engine a3ae39b, P4c slice 1 — engine `jsonrpc-compat.md` §3 calls rows +
  §5 block, the registration note naming this connector batch for the two
  new code positions) land their connector pipe —
  `calls.linkPresentation` {accountId, rootKey(16/21B b64)} →
  {presentation}, `calls.createLinkAuth` {accountId, rootKey, roomId(32B
  b64)} → {presentation, callLinkPublicParams}, `calls.groupProof`
  {accountId, groupId(32B b64)} → {proof},
  `calls.groupMembers` {accountId, groupId} → {members:
  [{uuid, userIdCipherText}]}, `calls.endorsementsPublicKey` {} →
  {endorsementPublicKey}. Pure passthrough: zero logic, zero parsing, zero
  logging — shape gate (the engine's own fail-closed b64 byte-budget gates
  mirrored onto the caller strings, answering INVALID_REQUEST locally
  before any upstream call) → engine method direct map → result bytes
  passthrough; the credential/presentation bytes cross the process exactly
  once, engine → host, and are never parsed, logged, or persisted (the
  relay-api-v2 §5 credentials-never-ride-host-HTTP ruling; the engine keeps
  the per-account credential cache). All five faces read lane,
  non-mutating (network possible inside one engine call, the
  conversations.getAvatar class; createLinkAuth's server-side POST is
  credential issuance with no user-visible state change);
  endorsementsPublicKey is the account-less face (the
  getStickerPackManifest anonymous-face discipline, the default group's
  engine). Two new code positions whitelist-registered:
  CALL_CREDENTIALS_FAILED (retryable=false — server rejection,
  zkgroup verification failure, the day's credential absent;
  error.data.status? = the server HTTP status's honest pass-through, the
  empty no-status object riding as itself) and CALL_CREDENTIALS_UNAVAILABLE
  (retryable=true — network/transport). The capability set does not grow:
  `call-signaling` widens in place (the §4.49/1.82 no-new-bit precedent —
  the faces are the signaling pipeline's own material supply); kt-engine
  only (KT-extension methods a stock signal-cli does not carry, the §4.69
  mode seam). Engine-side architecture ruling citable from engine compat
  §5: the credentials never transit the host's HTTP plane
  — previously 1.87 (2026-10-09; the cloud-backup upload passthrough
  batch (§4.72): the engine's upload half (engine cloud-backup-contract §6,
  v2 2026-10-08 — the five rulings and the measured pins; engine
  `jsonrpc-compat.md` §3 per-method rows) lands its connector pipe —
  `backups.uploadToCloud` `{accountId}` → `{status: "started"}` and
  `backups.uploadStatus` `{accountId}` → the progress snapshot mirror. The
  connector owns the chain's first stage: it (re)builds the §10.2
  transitional artifact `backup-export-input.ndjson` in the engine's
  per-account data directory with the §4.46 machine verbatim (write-once,
  rebuilt before every attempt; a failed build removes the partial file
  and answers before any engine traffic) and then forwards the start —
  send lane, account-scoped mutating, an indeterminate outcome answering
  SEND_OUTCOME_UNKNOWN and never retrying (a retry is the caller's fresh
  start: a new form, a new artifact, the server keeping the latest
  successful upload). The eight-family structured error table mirrors the
  engine's `CloudUploadError::rpc_code` single mapping source — shared
  with the uploadStatus failed phase so the two ends cannot drift: the
  five state families (BACKUP_AUTH_FAILED / BACKUP_NOT_PROVISIONED /
  BACKUP_ARTIFACT_MISSING / BACKUP_ARTIFACT_INVALID /
  BACKUP_UPLOAD_ALREADY_RUNNING) stay content-free retryable=false, the
  three message families (BACKUP_TRANSPORT_FAILED / BACKUP_UPLOAD_FAILED
  retryable=true, BACKUP_IO_FAILED retryable=false) carry the engine's
  `data.detail` verbatim. Two per-face divergences are reasserted at the
  upload call site: BACKUP_IO_FAILED is retryable=false with detail here
  while its §4.46 export face stays retryable=true content-free (the face,
  not the code, owns the face), and BACKUP_TRANSPORT_FAILED keeps its
  restore text on the restore path while the upload path names the upload
  chain. The two new code positions (BACKUP_UPLOAD_ALREADY_RUNNING,
  BACKUP_UPLOAD_FAILED) register in the structured-error whitelist — the
  registration the engine's compat rows assign to this batch. The
  capability set does not grow: `backup-export` widens in place (the 1.82
  call-signaling precedent — the upload's entire input is the
  backup-export artifact chain); kt-engine only (the §4.69 mode seam).
  `backups.uploadStatus` is the read-lane mirror of the engine's
  process-wide single-slot snapshot: `{present: false}` when absent (never
  started / succeeded — a success leaves no trace — / the engine
  restarted), else `{present: true, phase:
  exporting|uploading|failed, bytesRead, bytesTotal?, errorCode?}` through
  the same shape-gate discipline as cloudStatus (an absent `bytesTotal`
  stays an absent key, a malformed present-face fails
  UPSTREAM_PROTOCOL_ERROR rather than claiming absence, a down engine or
  an indeterminate read honestly answers `{present: false}`). Deliberately
  out: the media tier and the subscription/level query face (the engine
  does not check levels — the server refuses honestly), the desktop
  settings-page upload entry (the follow-up batch), a consumption watcher
  (the upload produces nothing local), and the export face's snapshot
  caretaker / skip-counter merge (the upload creates no snapshot and has
  no status face to merge counters into))
  — previously 1.86 (2026-10-08; the story-anchored attachment byte
  faces (§4.71): `stories.downloadAttachment` +
  `stories.openAttachment` — the byte half of the story surface. Stories
  land in no conversation row (§4.67), so the row-anchored
  `attachments.download` (§4.54) can never address one and there is no
  connector-side story row to resolve against (the connector keeps no
  story state — the §4.67 discipline). The shape is therefore
  caller-supplied: the desktop passes the pointer face its
  `story.received` event carried — `{accountId,
  attachment: {id, size, locator: {key, cdnKey, cdnNumber,
  uploadTimestamp?, digest?, plaintextHash?}}}` — and the connector gates
  every field fail-closed before any network traffic in three layers (the
  params serde gate: types, required key/cdnKey/cdnNumber,
  deny_unknown_fields so the whole rendering projection — contentType,
  filename, width, height, caption, isVoiceNote — is a params error, not
  a silent strip; the prepare ladder: the §4.54 locator-ingest caps
  mirrored onto the caller strings (key/cdnKey/digest ≤ 512 chars,
  plaintextHash ≤ 128 chars, uploadTimestamp ≥ 1 when present), the
  sanitizeId-parity landing-id gate, the 1..=104857600 size bound; the
  chain: the §4.38 step-3 `TransitFace::build` crypto-shape decode —
  base64 64-byte key set, one integrity face present — still ahead of the
  first byte on the wire). Nothing is invented (an absent pointer field
  stays absent — a hole answers INVALID_REQUEST, never a guessed
  download) and nothing is logged (pointer material never crosses an
  error or a log line; the failure family is content-free by
  construction). Everything after the gates is the §4.38 step-3 machine
  verbatim — the same chain history.attachmentDownload and
  attachments.download run (engine attachv2 pipeline "reuse" means the
  connector reuses the CDN download mechanism and its crypto; the engine
  stays metadata-only, E2 keeps projecting pointers, zero engine-repo
  changes). Result `{sizeBytes: N}`; already-present files answer the
  measured size without touching the CDN (the 1.57/1.61 idempotent-dedup
  posture). `stories.openAttachment` is the handle mint over the landed
  file — the messages.attachments.open machine without the row
  addressing (read lane, no engine touch); chunk read and release ride
  messages.attachments.readChunk/closeHandle unchanged (the handle, not
  the addressing, is the origin authority). Zero new error codes (the
  INVALID_REQUEST / UPSTREAM_ERROR families verbatim; download/verify
  failures terminal — never auto-retried, the AGENTS.md red line; local
  IO retryable). The capability set does not grow: `stories` extends in
  place (the 1.82 call-signaling precedent); mode-independent — the
  chain is pure connector, so no capability seam exists to answer.
  Deliberately out: media-tier story size profiles (the 100 MiB
  transit ceiling already bounds every caller), story text/preview
  attachment dispatch (the same face serves them — the pointer is the
  pointer), and the desktop viewer wiring (its own batch)
  — previously 1.85 (2026-10-08; the story view receipt batch
  (§4.70): the stories watch receipt lands both legs. Send leg:
  `stories.markViewed` `{accountId, storyAuthor, storySentAt}` →
  `{status: "sent"|"unknown"}` — the `messages.markViewed` receipt class
  verbatim (best-effort, any upstream failure degrades, nothing retries)
  with the §4.29 row selection replaced by the story anchor frame: exactly
  one upstream `sendViewedReceipt` call `{account, recipient (the trimmed
  author), timestamps: [storySentAt]}` — a story receipt names exactly one
  story, so there is no author fan-out and the 100-timestamp chunk framing
  never engages. `storySentAt: 0` answers INVALID_REQUEST (the official
  face has no meaningful "mark nothing viewed"), the author rides the
  opaque-id bound; mode-independent — the upstream face is the pinned
  upstream's own method, so no capability seam exists to answer, and the
  capability set does not grow (`stories` extends in place, the 1.82
  call-signaling precedent). Receive leg: the upstream receiptMessage
  carries no story bit on the wire (`{timestamp[], when, isDelivery,
  isRead, isViewed}`, compat §3), so the split happens at the local row
  face only — a VIEWED receipt is intercepted before the conversation
  ladder: with a resolvable source, an existing direct conversation still
  runs the §4.29 fold unchanged (message viewed receipts keep one
  statusChanged per real transition), timestamps that hit outgoing rows —
  decided by the hit rows, not the moved rows, so a replayed viewed
  receipt never leaks onto the story face — are consumed, and the
  leftovers ride exactly one transient `story.viewed` event
  `{accountId, sentTimestamps, viewedAt, viewer?, viewerAci?, viewerName?}`
  (absent optional keys stay absent); a conversation-less viewer forwards
  the whole receipt as story candidates (where the pre-1.85 pipeline
  evaporated the envelope), a source-less envelope drops whole (宁缺勿错),
  and the stories-disabled gate does not apply (it gates incoming story
  material, not receipts about the account's own stories). The event is
  rowless and transient — no row, no self-heal, the story.received
  discipline; the desktop's own timeline join stays the authoritative
  anchor, the connector only claims candidacy)
  — previously 1.84 (2026-10-08; the cloud-restore consumption batch
  (§4.69 记档): the restore's NDJSON product joins the §4.38 import pass —
  exactly the batch 1.83 deliberately left out. The engine writes
  `history-import-cloud.ndjson` whole into the registry-recorded account
  directory (the transfer archive's own directory) strictly after its whole
  chain succeeds, so the consumption watcher the start face now spawns (on a
  definite `started`, and on SEND_OUTCOME_UNKNOWN which may actually have
  started — the §4.68 watcher posture) reads completion from the artifact's
  disk freshness face, not from `cloudStatus`: the engine's snapshot clears
  on success, making `{present: false}` the same answer a never-started or
  restarted engine gives, while the `(mtime, len)` face captured at spawn
  changes only when the engine writes fresh — a stale archive from a
  previous restore is never imported, and a re-restore's rewrite re-arms
  the import (its rows dedupe onto the same identities, surfacing in the
  honest `skippedMessages` counter). `cloudStatus` rides the watcher loop
  for the failure face only: a `failed` phase (the engine's terminal
  snapshot) lands the honest engine-class failed ledger row so
  `history.importStatus` renders it and `history.importRetry` answers
  NO_ARCHIVE — retry is re-invoking `backups.restoreFromCloud`, the §4.68
  official-Retry semantics. The artifact-face check precedes the status
  check each tick (the engine writes the file strictly before an export
  failure maps onto the failed phase, so a landed artifact is consumed
  before a late failure observation could clobber a completed ledger row);
  the watch bounds at the §4.68 cadence (~3 s polls) under a 60-minute
  ceiling (the watched chain includes the whole CDN download), whose escape
  makes one last-chance disk check before landing the failed row. Status
  face: the §4.38 `history.importStatus` composition reused as-is — no new
  keys, the ledger row, the masked face, and the retention convergence are
  the §4.38/§4.68 faces unchanged (the same import pass consumed the
  artifact, so a second status key would fork one store truth into two
  faces the desktop must join). No wire change, no schema store version
  change, no capability change (both faces already ride `history-import`);
  recorded recovery hole: a connector restart mid-restore loses the watcher
  and the archive sits unconsumed until the next explicit
  `backups.restoreFromCloud` — the §4.38 link-time sweep stays
  transfer-archive-scoped on purpose, because one per-account ledger row
  cannot mark which artifact a completed row consumed)
  — previously 1.83 (2026-10-08; the cloud-backup restore passthrough
  batch (§4.69): the engine-side cloud restore (engine cloud-backup-contract
  §2, worktree feat/cloud-restore 9090ec5) lands its connector pipe — two
  faces, no local state, no consumption wiring. `backups.restoreFromCloud`
  `{accountId}` → `{status: "started"}`: the whole chain (backup credential
  fetch with the 404 bootstrap self-heal → zkgroup redemption/presentation →
  unauthenticated chat gRPC backup location → CDN download → AEP decrypt →
  NDJSON) runs in the engine's background, so the face is the
  `backups.export` class verbatim — send lane, account-scoped mutating, an
  indeterminate outcome answers SEND_OUTCOME_UNKNOWN and never retries, the
  terminal state is observed through the status face. The engine's
  `CloudRestoreError::rpc_code` table (the single mapping source) passes
  through verbatim as seven host codes: BACKUP_NOT_PROVISIONED /
  BACKUP_AUTH_FAILED / BACKUP_RUNNING (retryable=false), BACKUP_TRANSPORT_FAILED
  / BACKUP_DOWNLOAD_FAILED (retryable=true), BACKUP_DECRYPT_FAILED /
  BACKUP_EXPORT_FAILED (retryable=false); the four message-carrying families
  carry `error.data {detail}` verbatim (the promised contract detail face —
  the 1.50 pass-through discipline, generalized to a fixed shape), the three
  state families stay content-free. `backups.cloudStatus` `{accountId}` is a
  read over the engine's process-memory progress snapshot:
  `{present: false}` when absent (never started, succeeded — success leaves
  no trace —, or the engine restarted), else `{present: true, phase:
  downloading|failed, bytesRead, bytesTotal?, errorCode?}`; a missing
  `bytesTotal` stays a missing key (honest uncertainty, never a zero), the
  failed phase persists until the next start overwrites it, and an engine
  that is down or a malformed snapshot face answers honestly — down degrades
  to `{present: false}` (the restart truth), malformed fails
  UPSTREAM_PROTOCOL_ERROR (a read that delivered junk knows nothing).
  Capability: no new bit — both faces ride `history-import`, kt-engine only
  (the §4.68 mode seam, CAPABILITY_UNAVAILABLE). Deliberately out of this
  batch: the produced `history-import-cloud.ndjson` joining the §4.38 import
  pass and the desktop settings entry (the engine contract's follow-up
  batches — recorded, not worked around); the fixture mirrors the negotiated
  wire face only, real-device restore is the engine batch's acceptance) —
  previously 1.82 (2026-10-08; the group-signaling batch (§4.49.2):
  the call signaling pipe grows the group leg in both directions. Inbound,
  the v1.51 「解析失败即丢」 discipline is revised for the envelope-level
  opaque face: opaque signaling whose (account, direct, sender) ladder
  resolves to nothing — the group-signaling shape, since the `CallMessage`
  proto carries no group identity and a group frame resolves only its
  sender — forwards as the new transient `call.groupIncoming` event carrying
  exactly the envelope's projectable fields (`accountId`, `sender`,
  `payload` = the opaque bytes, `urgency`, `destinationDeviceId` when the
  engine projected one) and deliberately no `conversationId`; resolved 1:1
  frames keep `call.incoming` byte-identical and the legacy frame families
  still drop (no group leg). Outbound, `calls.sendSignal` routes by the
  conversation kind with no new parameter: a group conversation forwards as
  the engine group face `{account, group, data, urgency}` — the sender-key
  fan-out over the engine's `send_message_to_group`, the official
  `#handleSendCallMessageToGroup` → `sendContentMessageToGroup` landing
  mirrored as a pass-through — and refuses the payload-less legacy family
  locally (official group signaling is opaque-only); direct keeps the
  1:1 face verbatim. Capability `call-signaling` widened in place (no new
  bit); the schema grows the event enum, the event def, and the sendSignal
  descriptions. The engine group arm is the in-flight engine batch of the
  same wave — the current engine face fails closed on the group shape and
  the connector surfaces that verbatim as `UPSTREAM_ERROR` until it lands
  (recorded, not worked around); `call.groupRingUpdate` stays out of this
  batch (RingUpdate is derived inside the RingRTC layer, which the connector
  neither runs nor parses — it lands with the ring batch; the relay v2 §3
  alignment note names it)) —
  previously 1.81 (2026-10-08; the history-import non-text expansion
  batch (§4.38): the engine NDJSON archive's seven backup5 row types now
  land — frame-level `adhocCall` call records and the ChatItem
  `payment`/`giftBadge`/`viewOnce`/`storyReply`/`poll`/`adminDeleted`
  faces (engine 592b1d3, compat §5 递补批; desktop 67abb977+42801981 fix
  the wire shapes). The connector consumes the engine's live-key
  projections with no import-only parse for view-once (`viewOnce` /
  `isErased` / `viewOnceInvalid` top-level markers, the single medium on
  the row's standard `attachments` list); `storyReply`'s body rides the
  receive ladder and its author resolves connector-side (outgoing = the
  peer key, incoming = the account, direct conversations only — the
  engine exports no story context; `sentTimestamp` pins 0, stories are
  never imported); `poll` aggregates votes connector-side onto the
  desktop's read-only face (per-voter `voterId`+`optionIndexes`,
  `terminated` as the boolean face of `hasEnded`, the official voteCount
  tally has no desktop timeline face); `payment` crosses note-only and
  `giftBadge` state-only lowercased — amounts, fees, the
  TransactionDetails blob and the receipt-credential presentation have no
  desktop render/data face (recorded projection gaps, never transported);
  `adminDeleted` flips the 1.33 tombstone flag with the status ladder
  untouched (the admin identity has no row face, a recorded v1 gap); the
  `adhocCall` frame is recognized and honestly skip-counted — the
  connector keeps no call-history store. All faces ride the additive rich
  channel; no schema, capability, or store-version change) —
  previously 1.80 (2026-10-08; the official-desktop local-backup
  snapshot import opening (§4.68): the host surface grows two methods —
  `backups.importSnapshot` and `backups.importSnapshotStatus` — that hand a
  user-selected official Signal Desktop snapshot directory (`metadata` + `main`
  under `SignalBackups/signal-backup-<ts>/`) to the engine's new
  `backup.importSnapshot` face (engine backup-export-contract §11). The engine
  verifies the snapshot's metadata against the account's own key material
  (version, IV shape, and the CTR-decrypted backupId compared byte-for-byte —
  a foreign account's snapshot is refused before any decryption work), then in
  the background decrypts `main` through the official `FramesReader` and
  streams the same NDJSON projection the link-time transfer archive lands at
  (`history-import.ndjson`, atomic tmp+rename). The connector watches the
  engine's status to a terminal state and then drives the *existing* §4.38
  import pass on the produced archive — one store ledger row, the same
  `history.importStatus`/`history.importRetry` faces, no new capability bit
  (rides `history-import`, kt-engine only). Engine structured codes
  (BACKUP_SNAPSHOT_MISSING / BACKUP_SNAPSHOT_METADATA_INVALID /
  BACKUP_SNAPSHOT_KEY_MISMATCH / BACKUP_SNAPSHOT_IO_FAILED /
  BACKUP_IMPORT_ALREADY_RUNNING) pass through verbatim as retryable=false
  except the IO face (retryable=true). No schema store version change) —
  previously 1.79 (2026-10-08; the history-import group-update
  notice opening (§4.38): the engine NDJSON archive's per-message
  `groupUpdate` face now lands as the same system row a live group
  update does — 「群聊信息已更新」 with the `is_group_update` rich flag
  and the first `title` detail's new title mirrored into `group_name`
  under the live bound, through the existing rich channel (no schema,
  capability, or store-version change). The row identity is the live
  receive form over the updater: outgoing = the account, anything else
  resolves the archive author as an incoming author resolves
  (unresolvable = skip-counted, never invented); the conversation
  title is deliberately not mirrored from the notice (the chat frame
  owns the current title). Whatever the archive direction says, the
  face lands as a system row; a row that also carries the timer face
  splits into the §4.41 timer row and the group row — official
  `additionalMessages` semantics. `details` missing/empty is no face:
  plain skip routing behind it. Re-runs converge through the identity
  discipline) — previously 1.78 (2026-10-08; the history-import contact card
  opening (§4.38): the engine NDJSON archive's per-message `contact`
  face now lands — the card parses into a bounded
  `NormalizedContact` (name parts with empty-stays-absent semantics,
  ≤8 phone/email entries each with a value ≤256 chars, the official
  HOME/MOBILE/WORK/CUSTOM type vocabulary with unknown/absent
  coercing to HOME per the official parse face, an optional label
  and organization), rides the row's `contact` wire field through the
  rich channel (the §4.37/§4.40 additive route — persisted in
  `rich_json`, no store schema bump), and the avatar byte pointer
  joins the descriptor list exactly as a live attachment does —
  metadata-only, its locator face lands in the internal
  `attachment_locators` column, nothing downloads. The card rides
  only whole: a non-object face, or one with no usable field (no
  name part, no entries, no organization, no avatar), answers no
  card and the bodyless row behind it keeps plain skip routing —
  never a placeholder contact row; entry-level malformation (an
  empty value) drops the entry, never the row. No host wire shape
  change beyond the additive `contact` row field; no capability
  change, no schema change, no schema store version change) —
  previously 1.77 (2026-10-08; the history-import sticker row
  opening (§4.38): the engine NDJSON archive's per-message `sticker`
  face now lands — the 1.35 pack identity parses with the live-receive
  bounds as one source of truth (`MAX_STICKER_*`: packId ≤64 chars,
  packKey ≤128 base64, stickerId within u32, emoji ≤32 and optional),
  the row's body presence counts the sticker face as its body, and the
  `data` byte pointer joins the descriptor list exactly as a live
  envelope's sticker image does — metadata-only, its locator face rides
  the internal `attachment_locators` column, nothing downloads. The
  face rides only whole: a missing identity field or a missing/malformed
  `data` pointer answers no sticker, and the bodyless row behind the
  dropped face keeps plain skip routing. Store-side the import batch
  writer now persists `sticker_json` (previously hardwired NULL — a
  latent write-face gap unobservable while the importer projected no
  sticker). No host wire shape change: the `sticker` field has ridden
  the message wire face since 1.35; no capability change, no schema
  change, no schema store version change) —
  previously 1.76 (2026-10-08; the stories batch — the send face
  `messages.sendStory` opens on the kt-engine arm (the engine's
  `sendStory` KT extension method, engine compat §3): a rowless send —
  no clientRequestId, no pending settlement, the synchronous
  `{timestamp, results}` projection is the whole answer, an
  indeterminate outcome answers SEND_OUTCOME_UNKNOWN without retry;
  `recipients` is the caller-computed expansion (1..1001 entries — the
  desktop owns v1 list storage), `distributionListId` mirrors the
  official isStoryDistributionId face fail-closed (absent = the My
  story zero UUID, otherwise a strict 36-char UUID lowercased),
  `textStyle` is the official TextDisplayStyle vocabulary (DEFAULT =
  the key omitted), `bodyRanges` ride the §4.62 face verbatim,
  `previewImage` a data URI within the avatar budget; JVM/native
  answers CAPABILITY_UNAVAILABLE (retryable=false). The receive face:
  story envelopes forward as the transient `story.received` event (the
  bounded storyMessage projection verbatim — no row, no timeline
  state, the desktop owns the story timeline), `dataMessage.storyReply`
  rides a normal row's rich payload, and the stories-disabled gate
  (the official getStoriesBlocked leg, default false) drops both
  silently when the account opts out — `stories.setDisabled` /
  `stories.getDisabled` are the connector-local toggle faces (store
  meta KV, every mode answers). Capability `stories`; the schema event
  enum grows `story.received` and the request surface grows three
  params defs; no schema store version change (the toggle rides the
  existing meta KV under schema 20)) —
  previously 1.75 (2026-10-08; the username write-face opening —
  `accounts.setUsername`'s kt-engine arm comes alive as the engine's
  two-hop `usernameReserve`→`usernameConfirm` orchestration, with
  `deleteUsername` riding `usernameDelete` (§4.66; the engine's username
  write-face batch: proof/link cryptography, AccountRecord 33/35
  storage persistence). Four structured codes join the host contract —
  USERNAME_TAKEN / USERNAME_RESERVATION_GONE (deterministic,
  retryable=false) and USERNAME_RATE_LIMITED / USERNAME_SERVER_FAILED
  (retryable=true); the honest partial-success face (the server
  confirmed but the engine's storage write failed) rides the existing
  STORAGE_* family. The JVM/native `updateAccount` face is unchanged.
  Schema gains the four codes in the error enum and the mode seam in the
  method descriptions; no wire shape change, no capability change, no
  schema store version change) —
  previously 1.74 (2026-10-08; the username read-face opening —
  the kt-engine arms of `accounts.getUsername` and `username.lookup`
  (§4.65) come alive against the engine's `getUsername`/`usernameLookup`
  readers (the engine's username read-face batch, engine compat §3:
  AccountRecord username(33)/usernameLink(35) storage projection and the
  anonymous server-lookup path), while the pinned signal-cli JVM/native
  arms keep their honest CAPABILITY_UNAVAILABLE debt (`accounts.getUsername`)
  and their pinned `getUserStatus --username` route (`username.lookup`).
  The upstream frame is mode-shared; the mode picks only the engine
  method name. No wire shape change, no capability change, no schema
  store version change) —
  previously 1.73 (2026-10-07; the read-only post-deadline
  convergence batch — the P1 follow-up in the 758 hang-reconnaissance
  lineage (P0 deadline-ization `40e2248`, gate process-group hygiene
  `632fb39`): the engine-side lifecycle of a late response after a
  caller deadline was audited end to end and found already safe, so
  this batch ships zero production change and pins the convergence on
  the test side instead. The lifecycle, with anchors: request ids are
  per-generation monotonic (`kt-N`, engine.rs:1191, counter at :1137 —
  handle clones share the counter, ids are never reused), the actor
  registers a pending entry only after the frame is queued to the
  stdin writer (engine.rs:1363-1369), a fired deadline drops the
  caller's receiver and spawns a Cancel that retires the entry
  (engine.rs:1205-1217 → :1390-1392), and every inbound response
  dispatches by exact id against that map — a miss (late, duplicate,
  or engine-invented) drops the line with an
  `unknownOrDuplicateResponseId` protocol warning
  (engine.rs:1672-1676), so a late read-only answer can never be
  consumed by a later unrelated request. Both select! orderings of
  Cancel-vs-response are safe (a response processed first fails its
  oneshot send silently — the receiver is already gone), a terminal
  actor drains every pending entry with the class-correct failure
  (engine.rs:1488-1494), and a watchdog restart is a fresh generation
  with fresh pipes and a fresh counter, so no late answer crosses
  generations. Test-side pins: the fixture grows a `slowRead` hook (a
  read answer 0.35 s late, the same cadence as the mutating
  350/351/352 slow family), a new engine-integration test drives
  deadline → `Timeout` → late answer dropped as an unknown id → a
  subsequent `version` resolving with its own response, and the two
  mutating slow-answer tests trade their blunt 400 ms sleep for the
  same deterministic `unknownOrDuplicateResponseId` poll (one shared
  helper). No wire change, no new method, no capability tag, no
  schema store version bump (schema 20 unchanged)) —
  previously 1.72 (2026-10-07; the §4.62 receive half — the bodyRanges
  pass-through batch: the pinned `JsonTextStyle` projection a peer's
  dataMessage carries (`textStyles: [{style, start, length}]`, the §4.20
  vocabulary over UTF-16 code-unit offsets) reaches every wire face that
  reads the row — the receive pipeline has consumed it since 1.25 (the
  per-entry shape gate: the five renderable styles pass, `NONE` and unknown
  values drop individually, start/length are u32-bounded, 64 entries cap,
  malformed entries never fail the message) and the store has persisted it
  in the row's `rich_json` since the same revision, but this batch records
  the full-stack pass-through contract and pins it with the dedicated
  round-trip tests: `message.upserted`, `messages.list`, and
  `messages.search` all carry the ranges verbatim on the flattened
  MessageRecord; an all-invalid list collapses to the key-absent plain row
  (the §4.20 all-default collapse — absence means unstyled, pre-1.25 hosts
  read byte-identical rows); a plain body never grows the key. No new
  method, no wire shape change beyond the key's now-guaranteed presence on
  every read face, no capability tag, no schema store version bump (schema
  20 unchanged; the kt-engine receive projection is the parallel
  engine-repo batch's to land, the same recorded-gap family the 1.70
  consume batch practiced — the connector consume side is ready either
  way) —
  previously 1.71 (2026-10-07; §4.63 — the desktop evidence-gate
  data-plane gap closes on the connector side: every `conversations.list`
  summary row and the single `conversation.changed` summary now carry
  `hasUserInitiatedOutbound`, a bare always-serialized boolean (the
  muted/pinned/isSelf discipline): true exactly when the conversation's
  whole history contains at least one outgoing data row — the official
  `sentMessageCount > 0` factor weight
  (`ts/util/isConversationAccepted.preload.ts:58,75`, AGPL read-only anchor)
  surfaced as a standalone bit, aligned with the official
  `hasUserInitiatedMessages` whole-history EXISTS semantics
  (`Server.node.ts:2389-2410`, `Interface.std.ts:486`,
  `conversations.preload.ts:4621` / `:3752-3755`,
  `maybeForwardMessages.preload.ts:192-201`), independent of any message
  window or the newest row. The desktop evidence gate (§18.4 of its official
  parity matrix, 2026-10-06 data-plane gap record) exhausted its three local
  read faces (`lastMessageDirection`, the paged outgoing rows, the
  `messageRequestAccepted` fold — whose true is three-ways overloaded) and
  recorded the missing decomposition; this batch ships exactly the missing
  bit, named per that record (`hasUserInitiatedOutbound` or a
  `sentMessageCount` passthrough were the recorded options). Derived at read
  time over the store's existing data — the same indexed EXISTS family the
  §4.58 derivation runs, reading the row's `sentMessageCount > 0` material
  directly (the messages table, not the stored `mrr_factors_json` stamp,
  which never carries a count); every counting logic stays read-only
  untouched. Absent store evidence (a fresh row, or any history
  the connector does not hold — link-time import gaps included) projects
  false, the honest-absent degradation. Pure wire addition: no rename, no
  removal, no shape change elsewhere — pre-1.71 hosts read byte-identical
  summaries without the key; no new method, no capability tag, no schema
  store version bump (schema 20 unchanged) —
  previously 1.70 (2026-10-07; §4.58 consume batch — the connector now
  consumes the official `syncMessage.messageRequestResponse` sync face on the
  receive path when its engine projects it: a well-formed mirror (known
  non-UNKNOWN enum member 1..=6, exactly one thread leg, the ServiceId UUID
  shape, the 32-byte standard-base64 group leg — everything else fails closed
  into the plain syncMessage skip) resolves its thread across conversations
  (the §4.37 view-once ladder for the direct leg, the group master key for the
  group leg, then refused — the request family is 1:1 only), stamps the
  authoritative `acceptResponseType` factor into the row's existing
  `mrr_factors_json` (a read-modify-write merge: the receive writer owns the
  accept pair, the listContacts enrich owns the trio, neither clobbers the
  other), and the refreshed summary rides `conversation.changed` — no message
  row, no system row (the groupUpdated-projection posture). Derivation
  priority: a stamped ACCEPT accepts ahead of every window weight (the
  official factor's first weight, `isConversationAccepted.preload.ts:33-39`);
  any other member keeps the window answer; no stamp keeps the 1.65 local
  fallback verbatim. The wire shape never changed — the desktop consumes the
  new authority with zero desktop-side edits. No new method, no schema shape
  change, no capability tag (the 1.65 key is already on every summary; the
  group-timer dated-note precedent: same-face consume closes without a wire
  revision of its own — the chain entry exists to date the consume). Engine
  modes: the pinned signal-cli and the kt-engine receive faces still drop the
  sync on the wire today (the recorded §4.58 engine gap for the projection
  half); the consume path activates automatically the moment an engine ships
  the projection — engine-repo work out of this batch's boundary (the "still
  drop" line is a write-time snapshot: the kt-engine projection has since
  landed, engine `bbf3087` — the `syncMessage.messageRequestResponse`
  receive projection — so kt-engine mode consumes live from that build on,
  while the pinned signal-cli face still drops the sync, an upstream gap);
  1.72 has since landed above (the §4.62 receive half, the bodyRanges
  pass-through batch — its entry tops the chain)) —
  previously 1.69 (2026-10-06; §4.62 — the send-side text-formatting
  face opens: `messages.sendText` gains an optional `textStyle:
  [{style, start, length}]` array beside the §4.29 `mentions` face — the
  official bodyRanges write face over the §4.20 vocabulary (BOLD / ITALIC /
  STRIKETHROUGH / MONOSPACE / SPOILER; signal-cli `--text-style`
  `start:length:STYLE` triples over UTF-16 code units, the JSON-RPC camelCase
  `textStyle` key). The style vocabulary fails closed INVALID_REQUEST — the
  sender's own ranges are caller-authored, so a hostile or buggy list is
  rejected, not silently rewritten like the §4.20 receive cap; past 64
  entries (the §4.20 receive cap mirrored) INVALID_REQUEST, never truncation;
  no body-bounds gate (the official `parseTextStyles` validates none and the
  desktop clips its own ranges — the connector neither checks nor clips).
  A styled send persists its ranges on the pending row's rich payload (the
  §4.36 view-once marker precedent — the sender's bubble keeps its formatting
  across restarts); retries rebuild the send without them (the §4.29 mentions
  deviation). Same-capability in-place evolution, no new capability tag.
  Engine modes: stock signal-cli carries `textStyle` natively; the kt-engine
  send face does not parse it yet, so an engine-mode styled send delivers
  unstyled to the peer while the sender row keeps its persisted formatting
  (recorded engine-batch debt family; the §4.20 receive face is unaffected
  since 1.25). 1.67 and 1.68 have since landed (the member-label read face,
  the §4.60 entry, and the safety-number faces, the §4.61 entry — both in
  the chain below), so this batch takes the next chain entry) —
  previously 1.67 (2026-10-06; §4.60 — the group member-label read
  face: `groups.get` roster entries now carry the upstream member's own
  `labelEmoji` / `label` pair, the pinned `listGroups` `JsonGroupMember`
  `@JsonInclude(NON_NULL)` fields captured verbatim at sync under the pinned
  server budgets (emoji ≤64B / string ≤512B, over-budget drops the key) and
  projected at read time as absent-when-none keys; same-day rebump —
  1.66 is the landed multi-attachment send face (the §4.59 entry, in the
  chain below — it landed first and its entry therefore sits in the chain,
  newest wins); 1.64 and 1.65 have since landed (the group-invites face
  §4.57 and the message-request receive projection §4.58, both in the
  chain below) and the safety-number faces have landed in this same stack
  (1.68, the §4.61 entry, also in the chain below), so this batch takes
  the next chain entry) —
  previously 1.68 (2026-10-06; §4.61 — safety-number faces `identities.get` /
  `identities.verify`: the pinned upstream's native identity faces — the read narrows the
  native `listIdentities` JSON-RPC projection (a `JsonRpcLocalCommand`, so the JSON-RPC read
  is native; jsonrpc.rs:121-126 + ListIdentitiesCommand.java:23) to the resolved peer
  reference and projects the pinned `JsonIdentity` record verbatim (empty array = the honest
  `identity: null`, the official SafetyNumberNotReady face), the write carries only the native
  `trust` `verifiedSafetyNumber` form (the official "Mark as verified" digits-compare
  semantic; the testing-only `trustAllKnownKeys` mode is never sent) with the official
  clear-verification half recorded as having no pinned JSON-RPC projection; Read + Send lanes,
  capability `safety-number` one bit for both halves. 1.64 is the landed group-invites face
  (the §4.57 entry, in the chain below) and 1.66 is the landed multi-attachment send face
  (the §4.59 entry, in the chain below) — both landed first and their entries therefore sit
  in the chain, newest wins; 1.65 has since landed (the message-request receive projection,
  the §4.58 entry in the chain below) and 1.67 has since landed in this same stack (the
  member-label read face, the §4.60 entry in the chain below), so this batch takes §4.61
  directly; 1.69 tops the stack (the text-formatting send face, the §4.62 entry)) —
  previously 1.66 (2026-10-06; §4.59 — the multi-attachment send face opens: `messages.attachments.send` gains
  an optional plural
  `attachments: [{dataBase64, sizeBytes, filename?, contentType?}]` form
  beside the §4.12 singular payload, mutually exclusive with it — the
  official forward face where one forwarded multi-attachment message is one
  upstream message. 1..32 entries (the upstream per-message ceiling
  mirrored), combined raw size within the §4.12 100 MiB message budget so
  one request still fits one host frame, per-entry validation the §4.12
  family verbatim, voiceNote/viewOnce stay singular-form faces, caption and
  quote are message-level. The upstream `send` has always accepted the
  array-typed `attachments` parameter — the singular §4.12 entry point was
  the only one-element gate; `dispatch_send` downstream wire shapes are
  byte-identical. Same-capability in-place evolution on the §4.30/§4.36
  precedent; the schema-19 store is untouched, zero capture-path change.
  1.63 is the landed message-request response face (the §4.56 entry, in the
  chain below — it landed first and its entry therefore sits in the chain,
  newest wins); 1.64 is the landed group-invites face (the §4.57 entry, in
  the chain below — newest wins), 1.65 is the landed message-request receive
  projection (the §4.58 entry, in the chain below — newest wins)) — previously
  1.65 (2026-10-06; §4.58 — the message-request receive projection: the
  conversation summaries (`conversations.list` page and the single
  `conversation.changed` summary) now carry `messageRequestAccepted`, the
  official `isConversationAccepted` multi-factor (upstream
  `ts/util/isConversationAccepted.preload.ts:21-82`, AGPL read-only anchor)
  derived at read time over the material this chain can honestly see. New
  store column `conversations.mrr_factors_json` (schema version 20, additive
  nullable): the throttled listContacts enrich stamps the engine-supplied
  {hidden, profileSharing, blocked} trio per direct row on the SAME one
  round trip it already spends on titles — signal-cli's `JsonContact` carries
  all three natively (`isHidden`/`profileSharing`/`isBlocked`), an engine
  without them (the kt-engine read face today) stamps the probed-absent `{}`.
  Honest per-factor holes, never invented: the ACCEPT response stamp has no
  source on either engine (the pinned signal-cli projects
  `syncMessage.messageRequestResponse` only on its plain-text path —
  `ReceiveMessageHandler.java:413-424` — never on the JSON-RPC wire
  `JsonSyncMessage.from`, and the kt-engine receive projection drops it), so
  the receive-side stamp the desktop §4.56 consume batch assumed is recorded
  as the engine gap; `messageCountBeforeMessageRequests` is official-desktop
  migration state no engine projects; groups carry no whitelisted/addedBy/
  pendingAdminApprovalV2 read face, so a group row derives accepted from its
  own outgoing window alone. The key is a bare always-serialized bool (the
  muted/pinned/isSelf discipline — both values are meaningful); pre-1.65
  hosts read one added key, and a desktop consumes the key as authoritative,
  keeping its local window derivation only for the key-absent pre-1.65
  connectors. The parallel batches landed while this entry was in flight —
  1.63 is the landed message-request response write face (the §4.56 entry, in
  the chain below — drafted as §4.55, renumbered on landing) and 1.64 the
  landed group-invites face (the §4.57 entry, in the chain below — newest
  wins)) — previously 1.64 (2026-10-06; §4.57 — the group invites face opens:
  `groups.get` now carries `pendingMembers` — the invited-but-not-yet-joined
  set — projected live at read time as bounded `{id, uuid?, name?}` entries
  from the same single-group upstream `listGroups` read the new
  `groups.revokeInvites` write folds against (capability
  `group-invite-revoke`, one capability for the read and write halves, the
  1.62 read-face discipline), and the write face itself: the safe managed
  revoke over the pinned upstream's revocation-woven updateGroup
  removeMembers semantics (`GroupHelper.updateGroupV2` discriminates three
  ways — pending members ride `revokeInvitedMembers`, full members ride the
  member removal, requesting members ride `refuseJoinRequestMembers` — so
  the connector folds the candidates against the engine's own
  `pendingMembers` read and only pending invitees can ever ride the array;
  a fully-stale fold answers `INVITE_NO_LONGER_PENDING` retryable with zero
  writes, an indeterminate outcome answers SEND_OUTCOME_UNKNOWN and is
  never retried, the kt-engine face answers CAPABILITY_UNAVAILABLE after
  local validation on the §4.53 engine-debt precedent). The read face
  mirrors the 1.62 requestingMembers reasoning point for point: live read,
  not a sync capture (the revoke fold's own read is the truth source, the
  rendered list and the folded write set are the same bytes moments apart;
  the sync path cannot carry this face), best-effort degradation without
  the key, the method itself ungated like every §4.8 cache read with the
  desktop gating on the capability + key presence. Zero local writes, the
  schema-19 store untouched, zero capture-path change. 1.63 (the
  message-request response face) landed ahead of this batch on the rebased
  stack — its §4.56 entry already sits in the chain and the 1.68 header
  above took the top, newest wins; this batch takes §4.57 directly) —
  previously 1.63 (2026-10-06; §4.56 — message-request response faces
  `conversations.acceptRequest` / `conversations.declineRequest`: the pinned upstream's native
  `sendMessageRequestResponse` sync face (accept/delete — the official CLI carries only this
  pair; the block/block-and-delete twins stay the desktop's explicit `conversations.setBlocked`
  compositions), Send lane account-scoped mutating, capability `message-requests`; direct
  conversations forward the resolved peer reference, groups the raw group master key; the
  pinned face answers an empty result on success so the result face is `{status}` — zero local
  writes, the request state converges through the §4.56 receive projection; same-day rebump
  past 1.62 — 1.62 is the landed group-approval read face (the §4.53 appendix, in the chain
  below)) — previously 1.62 (2026-10-06; §4.53 appendix — the group join-request
  read face opens: `groups.get` now carries `requestingMembers`, the same
  single-group upstream `listGroups` read the 1.58 approval fold runs against,
  projected live at read time as bounded `{id, uuid?, name?}` entries — present
  only when the engine answers a non-empty requesting set, absent otherwise.
  The key is deliberately a live read, not a sync-captured cache field: the
  1.58 record fixed the approval truth source as the fold's own in-call
  `listGroups` read, and this face consumes the same source so the list the
  desktop renders and the set the write folds against can never disagree — a
  `group_extra_json` snapshot would be a second, older truth the write's own
  `JOIN_REQUEST_NO_LONGER_PENDING` stale ladder exists to paper over, and the
  sync path cannot carry this face anyway (its 60s short-circuit would keep
  answering the pre-approval roster after an action). This is the desktop
  consume batch's data face for the 1.58 write (`groups.approveJoinRequests`,
  capability `group-join-approve` unchanged — one capability, read and write
  halves together, the 1.53 appendix discipline); the schema-18 store is
  untouched, zero capture-path change) — previously 1.61 (2026-10-06; §4.54 — live message-attachment pointer persistence +
  `attachments.download`: the receive envelope's transit locator face persists into a new internal
  `messages.live_attachment_locators` column (schema version 19) beside the byte-identical wire
  descriptors when an engine projects it (the pinned signal-cli 0.14.8 and the kt-engine receive
  face project metadata only today, so live rows carry the honest hole until an engine ships the
  projection), and the new `attachments.download` face (Send lane, account-scoped mutating,
  capability `attachment-download`) walks the stored face positionally — attachmentIndex
  addressing, live/import locator ladder, the §4.38 step-3 chain unchanged (idempotent dedup,
  terminal failures, no auto-retry). Same-day rebump past 1.60 — 1.58
  (group-join-approve) and 1.60 (summary isTerminated projection) landed on main before this
  batch; 1.59 belongs to the parallel username-passthrough batch and has since
  landed — it takes the next chain entry) — previously 1.59 (2026-10-06; §4.55 — the Signal username faces:
  `accounts.setUsername` / `accounts.getUsername` / `username.lookup`, capability
  `username`; the write rides the pinned `updateAccount` mutually exclusive
  group with the official desktop username grammar mirrored fail-closed
  locally, the read honestly answers CAPABILITY_UNAVAILABLE because the
  pinned signal-cli has no local-account username getter, and the lookup
  resolves usernames/username links through `getUserStatus --username` with
  a 20-entry KT batch bound; previously 1.60 (2026-10-06; §4.52 conversation-summary/contact-row
  terminated projection — the `conversations.list` page, the single
  `conversation.changed` summary, and the `contacts.list` rows now carry the
  group termination flag the 1.53 §4.50 appendix sync capture stores, read out
  of `group_extra_json` at projection time as a plain bool collapsed to an
  absent key when false; the desktop forward-target filter consumes it to keep
  terminated groups out of the forward candidates, the official
  `selectors/conversations.dom.ts` `!conversation.terminated` behavior;
  1.57/1.58/1.59 were reserved by parallel batches — 1.58/1.59 never landed
  on this tree, 1.57 landed as the §4.38 attachv2 step-3 download batch below;
  this revision rebumps from 1.57) — previously 1.58 (2026-10-06; group
  join-request approval — `groups.approveJoinRequests`: the safe managed write over
  the pinned upstream's approval-woven updateGroup member semantics; capability
  `group-join-approve`; §4.52→§4.53 renumbered on landing, the batch drafted before
  1.59/1.60 claimed §4.52 first) — previously 1.57 (2026-10-06; §4.38
  attachv2 step 3 — history attachment download: `history.attachmentDownload`
  walks one stored pointer face end to end (transit-CDN stream → ciphertext
  HMAC/digest gates → AES-256-CBC decrypt → log-pad trim → plaintextHash gate
  → governed media directory landing where `messages.attachments.open`
  reads), consuming the schema-18 locator column; same-day rebump from
  1.56, which collided with 1.54 §4.51 and 1.53 §4.50 the same way — one
  contract revision per batch, newest wins the header, history kept in the
  bullets) — previously 1.56 (2026-10-06; §4.38 attachv2 — link-time import
  attachment pointer faces: the engine's NDJSON export stops discarding
  `FilePointer.LocatorInfo` (transfer archives carry no media bytes; the
  pointer three-piece key/transit-CDN/integrity is what makes media
  recoverable after import, per the official import pipeline), the connector
  parses it into a new internal `messages.attachment_locators` column (schema
  version 18) beside the byte-identical wire descriptors — ingest only that
  batch, the download wiring (step 3) is registered as follow-up; 1.55 was
  §4.49.1 call-signaling legacy bare-frame face + urgency pass-through
  (`calls.sendSignal` payload becomes optional for the legacy busy/hangup bare
  frames and the `urgency` parameter rides through to the engine), itself
  rebumped from 1.53 which collided with the same-day 1.54 §4.51 (link-time
  import progress + manual retry) and 1.53 §4.50 appendix (groups.get
  isTerminated read face); 1.52 was the §4.50 group-termination write batch,
  whose Status-header bump covered the 1.51 call-signaling miss)
- Connector source baseline: `main` @ `6656f70`
- Target engine baseline: unmodified `signal-cli v0.14.8` (upgraded from 0.14.7 on 2026-09-23
  per `docs/signal-cli-upgrade.md`: smoke 4/4 on JRE 25; 0.14.8 adds voice-note metadata and
  GroupsV2 end-group exposure, no JSON-RPC field removals)
- Target Java baseline: JRE 25
- Initial platforms: Windows 10/11 x64, macOS x64, macOS arm64
- Remote publication: not authorized yet; all work remains local until testing and review are complete

This document is the standalone implementation plan for `kt-signal-connector`. KT Desktop owns the
product UI and business features; this repository owns the isolated local transport, process,
storage, and normalized message boundary.

## 2. Delivery Decision

The production process model is:

```text
closed-source KT Desktop
        |
        | authenticated, versioned local IPC
        v
open-source kt-signal-connector (Rust)
        |
        | stdin/stdout JSON-RPC 2.0
        v
Signal engine child process
  - production: self-hosted kt-signal-engine (Rust, vendored presage stack)
  - fallback: pinned signal-cli + JRE (development and behavior
    comparison only, ADR 0003)
        |
        v
Signal service
```

Per local KT profile the fixed process count is:

```text
1 x kt-signal-connector
G x engine child processes (kt-signal-engine in production; the pinned
  signal-cli JVM/native runtime in the development/comparison fallback),
  one per proxy group (G >= 1, hard ceiling 8)
N x Signal accounts
M x conversations
```

Accounts and conversations do not create processes, and accounts do not create engines either: an
account joins one launcher-defined proxy group at link time and shares that group's engine (ADR
0001, see 4.4). The default single-group deployment (G = 1) is exactly the original model. One
ordinary text conversation is expected to add only its bounded UI/message window in KT; it does not
consume a separate engine or 1 GB of memory.

## 3. Ownership Boundaries

### 3.1 KT Desktop

KT Desktop owns:

- Signal account, conversation, message, and composer UI.
- AI reply, target-language policy, translation, remarks, and quick replies.
- Renderer-to-main-process allowlisted IPC.
- Connector artifact download, signature verification, activation, and last-known-good rollback.
- Tenant capability and product rollout switches.

KT Desktop must not own:

- signal-cli process protocol parsing.
- Signal account keys or protocol databases.
- raw signal-cli envelopes.
- connector source or AGPL dependencies linked into its executable.

### 3.2 Connector

The connector owns:

- authenticated local IPC and API version negotiation.
- signal-cli child process lifecycle and JSON-RPC request matching.
- link flow state and QR secret lifetime.
- raw-envelope validation and normalization.
- stable message IDs, deduplication, SQLite indexes, cursors, and bounded caches.
- attachment metadata and any approved file-handle boundary.
- runtime health, RSS/CPU monitoring, redacted diagnostics, and engine restart policy.

The connector must not:

- render UI or accept arbitrary renderer connections.
- execute unrestricted KT feature code.
- expose arbitrary signal-cli JSON-RPC methods.
- expose raw envelopes, data-directory paths, account keys, or arbitrary file paths.
- link or copy signal-cli/libsignal source.

### 3.3 signal-cli

signal-cli exclusively owns:

- Signal protocol and cryptography.
- linked-device account state and keys.
- upstream websocket communication.
- Signal-specific send and receive behavior.

The first release uses an unmodified, pinned upstream artifact. A required upstream patch is a new
architecture and license review, not a routine implementation detail.

## 4. API Boundary

### 4.1 Transport

- Windows: a random per-start named pipe restricted to the current user SID.
- macOS/Linux: a socket in a profile-private directory with mode `0600`. The listener binds a
  staging sibling, hardens it, then publishes it with a rename, because the host connects as
  soon as the endpoint path appears and must never reach a socket that is still world-connectable.
- No public listener and no fixed localhost HTTP/TCP port.
- The renderer never receives the endpoint or the session secret.
- A host that closes its connection abruptly ends the session exactly like a clean EOF: the
  process still exits zero. Only an unparsable or oversized frame is a protocol failure.

Electron Main creates a 256-bit random bootstrap secret. On macOS/Linux it writes an owner-only
temporary file and the connector reads and deletes that file before accepting a client. On Windows
Main writes the exact secret to the child process's inherited anonymous stdin pipe and closes it;
the Windows connector refuses file bootstrap because POSIX mode bits do not prove a private DACL.
The first frame performs a challenge-response handshake and negotiates API capabilities. The
`capabilities` array is every dispatchable method name in table order plus the feature tags; since
contract 1.98 the `attachment-download` tag is the one conditional member — it rides the array
only when the runtime's media ingest is armed (the media table present, the exact gate every
media method answers), so a connector without it never advertises a face that would answer
`CAPABILITY_UNAVAILABLE` for every download and the desktop takes the honest metadata-only path.
The secret and endpoint are never logged, and their mutable buffers are zeroized after
bootstrap/handshake.

The bootstrap payload is two lines (Phase 3 key contract, 2026-08-26): line 1 is the bootstrap
secret, exactly 64 lowercase hexadecimal characters, unchanged; line 2 is the 32-byte store key,
also exactly 64 lowercase hexadecimal characters, generated once by the desktop and persisted via
Electron `safeStorage`. Read-and-delete (file) and zeroization (memory) cover both lines. The store
key never crosses the socket, never enters the wire protocol or the schema, and is never logged;
the connector holds it in `Zeroizing` memory only. `KT_SIGNAL_STORE_KEY` is a dev/test override and
never set in production. A single-line legacy payload is still parsed, but without a store key the
connector fails closed: an existing plaintext store is never opened without a key, and with no
store at all startup is refused until the desktop generates and delivers the key (first run).

The server sends a random
32-byte hexadecimal `serverNonce`; KT answers with a random 32-byte hexadecimal `clientNonce` and:

```text
proof = hex(HMAC-SHA256(secret,
  "kt-signal-connector-v1\0" + serverNonce + "\0" + clientNonce + "\0" + apiVersion))
```

Nonces and proofs must be canonical lowercase hex. Authentication has a five-second deadline and a
connection is closed after any malformed or failed handshake. Phase 1 supports exactly API `1.0`.

### 4.2 Framing

The initial host protocol uses newline-delimited JSON with explicit maximum frame size. Each request
contains:

```json
{
  "apiVersion": "1.0",
  "requestId": "opaque-id",
  "method": "runtime.status",
  "params": {}
}
```

A response contains exactly one of `result` or `error`. Events have no request ID and use a separate
`event` field. Unknown fields are ignored only when the negotiated minor version permits them;
unknown methods are rejected.

### 4.3 Initial allowlist

Phase 2 host methods:

- `handshake`
- `runtime.status`
- `runtime.start`
- `runtime.stop`
- `accounts.list`
- `accounts.deleteLocalData`
- `link.start`
- `link.finish`
- `link.cancel`
- `conversations.list`
- `messages.list`
- `messages.getText`
- `messages.sendText`
- `contacts.sync`
- `contacts.list`
- normalized runtime/account/conversation/message events

No generic `call`, `exec`, `jsonRpc`, file-read, URL-open, or raw-envelope endpoint is allowed.
The machine-readable Phase 2 envelope contract is
[`schemas/connector-api-v1.schema.json`](../schemas/connector-api-v1.schema.json). Methods outside this
allowlist return `METHOD_NOT_ALLOWED`. Media and bulk automation remain unavailable.

### 4.4 Proxy groups (Phase 4 contract, 2026-08-26; ADR 0001)

Proxy groups evolve API `1.0` in place; the apiVersion handshake binding is unchanged. Every change
is additive for a caller that never names a group: such a caller gets exactly the pre-Phase-4
behavior, with all of its accounts in the reserved `default` group.

- Groups are launcher input, never IPC input: repeatable `serve --proxy-group <id>=<host:port>` or
  the `KT_SIGNAL_PROXY_GROUPS` comma-separated equivalent, governed by the signed runtime manifest
  like every other process input (see 5). Group ids match `^[a-z0-9]([a-z0-9-]{0,30}[a-z0-9])?$`;
  `default` is reserved and always exists. The legacy global `--socks-proxy` /
  `KT_SIGNAL_SOCKS_PROXY` configures the `default` group's proxy and must not be redefined in the
  group list. When the flag and the environment variable are given together they merge into one
  launcher-ordered list (flag entries first, then environment entries); a group id that appears in
  both sources — or twice anywhere — aborts startup: ambiguity is refused fail-closed and never
  resolved by precedence, and neither source may name `default` itself. More than 8 groups, a
  malformed id, or a malformed proxy value aborts startup with a clear error.
- `link.start` params gain an optional `proxyGroup` (opaque id). Absent means `default`. An unknown
  id fails with `PROXY_GROUP_NOT_FOUND` (`retryable=false`). The result gains a `proxyGroup` field
  echoing the selected group. The binding is fixed when `link.finish` succeeds and is immutable for
  the life of the account; moving an account means `accounts.deleteLocalData` plus a fresh link.
- Link flows are per group: `LINK_IN_PROGRESS` applies within one group only, the `link.finish`
  wait lane is per group, and a `link.cancel` that must restart an engine restarts only that
  group's engine (see 6.1).
- `accounts.list` result items, the `link.finish` result, and `account.changed` event data gain a
  `proxyGroup` field (always present; accounts linked before Phase 4 read as `default`).
- `runtime.status`, `runtime.start`, and `runtime.stop` results gain `proxyGroups`: an array in
  launcher config order with one entry per group,
  `{groupId, state, pid?, rssBytes?, resourcePressure, accountCount}`, where `state` is
  `running|stopped|exited|faulted` and `accountCount` counts the accounts bound to the group. The
  top-level aggregate is preserved for existing callers with a pinned rule: `state` is `running`
  if any group is running, else `faulted` if any is faulted, else `exited` if any is exited, else
  `stopped`; `rssBytes` is the sum of live group samples; `resourcePressure` is true when any
  group is in pressure; the top-level `pid` is present only when exactly one group exists. For a
  single-group deployment this aggregate is field-for-field identical to the pre-Phase-4 result.
  `runtime.start` fails with `RUNTIME_START_FAILED` only when no group engine started.
- New event `proxyGroup.stateChanged` with data `{groupId, state, pid?, rssBytes?,
  resourcePressure}` on every group engine transition. `runtime.stateChanged` keeps carrying the
  top-level aggregate with its shape unchanged. `runtime.resourcePressure` event data gains a
  required `groupId` (`default` in single-group deployments). Unknown event names and unknown
  fields are ignored by existing hosts; this follows the precedent of `message.statusChanged`
  gaining a producer while older hosts only routed it.
- Account-addressed methods (`messages.*`, `conversations.list`, `contacts.*`,
  `accounts.deleteLocalData`) route by `accountId` to the account's group engine; their request
  and response shapes are unchanged.
- Dormant groups: an account whose stored `proxyGroup` is not part of this launch plan stays
  intact but unreachable. Every account-addressed method naming it fails with
  `CAPABILITY_UNAVAILABLE` (`retryable=false`; the message carries the group id, never an
  endpoint), and the account does not appear in `accounts.list`, which unions configured groups
  only. The connector deliberately never auto-starts a group outside the launch plan: a group's
  proxy exists only as launcher input (above), so inventing one at runtime could route the
  account through the wrong or missing egress and break anti-association (ADR 0001 R1).
  Restoring the group to the launch plan makes its accounts reachable again unchanged; the
  store-level delete-replay closure in 6.2 is the one deliberate exception that survives a
  vanished group.
- Deliberately absent from the wire: group creation/reconfiguration/deletion, group reassignment,
  per-group `runtime.start`/`runtime.stop`, and proxy endpoints. The desktop allocated the local
  proxy ports and already knows the group-to-proxy mapping; the wire carries opaque group ids only,
  and error messages never contain a proxy host:port.
- Version skew: a group-aware host detects support by the presence of `proxyGroups` in the
  `runtime.status` result before ever sending `proxyGroup`; against a pre-Phase-4 connector the
  unknown param is rejected with `INVALID_REQUEST` (`deny_unknown_fields`) and fails closed, never
  silently landing the account in the wrong egress.

### 4.5 messages.remoteDelete (contract revision 1.6, 2026-09-02)

API `1.0` evolves in place again (§4.4 precedent); the apiVersion handshake binding is unchanged.
The method is additive: a host that never calls it sees no change, and a host must detect support
through the handshake `capabilities` array — calling it against an older connector answers
`METHOD_NOT_ALLOWED`. Design and rationale: `docs/remote-delete-l2-plan.md`.

- `messages.remoteDelete` remotely deletes ("Delete for everyone") one message the linked account
  itself sent. Params: `accountId`, `conversationId`, `messageId` (all required, opaque ids) and an
  optional `operationId` that the connector validates by shape but never persists — there is no
  operation ledger and no store migration. Addressing is by `conversationId` only: the target must
  already exist in the local history.
- Eligibility: only a local addressable outgoing row (`sent`/`delivered`/`read`) qualifies; its `sentAt` was
  overwritten with the send response's upstream Signal timestamp and becomes the delete's protocol
  identity (delivery/read receipts preserve it). Pending/failed/unknown rows carry only a local clock value, and incoming rows are not
  the account's own messages — all of them answer `MESSAGE_NOT_FOUND`, exactly like quote
  resolution. The conversation's kind selects the upstream addressing: `recipient: [peerKey]` for
  direct chats, `groupId: peerKey` for groups (signal-cli jsonRpc `remoteDelete` params
  `account`/`targetTimestamp`/`recipient`/`groupId`, verified against the pinned 0.14.7
  distribution).
- Result: `{"status": "deleted"}` when signal-cli confirmed the delete, or
  `{"status": "unknown"}` when the outcome of the mutating upstream call is indeterminate. The
  `unknown` semantics equal every `*_OUTCOME_UNKNOWN` error: the connector never retries
  automatically; a host may explicitly re-send with a fresh requestId and the same `operationId`,
  and the real outcome is whatever the peer's rendering shows (best effort, 24h upstream window).
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `MESSAGE_NOT_FOUND` (missing or ineligible row), `RUNTIME_NOT_RUNNING`,
  `INVALID_REQUEST`, `UPSTREAM_EXITED`, and `UPSTREAM_ERROR` for an explicit upstream rejection
  (expired window, already deleted, not deletable) — its `retryable=true` is the global mapping,
  and hosts must not auto-retry deletes on it.
- Known behavior boundaries: the local `messages` row is intentionally left unchanged on a
  successful delete (presentation/bookkeeping belongs to the desktop, which receives
  `status: "deleted"`); deletion events from peers or the account's other devices are not yet
  converged locally; nothing is emitted to `message.statusChanged` by this method.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account `sendText`/`remoteDelete`/`contacts.sync`/`deleteLocalData` serialize), the
  delete drain barrier, and the per-account request budget. Metrics classify it as `send`.

### 4.6 messages.sendReaction (contract revision 1.7, 2026-09-03)

API `1.0` evolves in place again (§4.5 precedent); the apiVersion handshake binding is unchanged.
The method is additive: a host that never calls it sees no change, and a host must detect support
through the handshake `capabilities` array — calling it against an older connector answers
`METHOD_NOT_ALLOWED`. It follows the `messages.remoteDelete` (§4.5) implementation shape end to
end; design rationale and the upstream-verification notes live in `docs/remote-delete-l2-plan.md`
§3.2.

- `messages.sendReaction` sends or removes a reaction on one message that already exists in the
  local history. Params: `accountId`, `conversationId`, `messageId`, `emoji` (all required; the
  first three opaque ids) plus an optional `remove` (boolean, default `false`) and an optional
  `operationId` that the connector validates by shape but never persists — there is no operation
  ledger and no store migration. Addressing is by `conversationId` only: the target must already
  exist in the local history.
- `emoji` must be a single unicode grapheme cluster of 1–32 UTF-8 bytes — the pinned signal-cli
  requirement (SendReactionCommand `--emoji` help text, verified via `javap` against the pinned
  0.14.7 distribution). Grapheme clustering keeps multi-codepoint emoji (ZWJ sequences,
  skin-tone modifiers, flags) valid while rejecting multi-emoji strings; the service enforces the
  rule deterministically.
- Eligibility and target identity: an own outgoing row qualifies in an addressable state
  (`sent`/`delivered`/`read`; its `sentAt` was overwritten with the send response's upstream Signal
  timestamp and preserved across receipts); an incoming
  row carries the envelope timestamp, so it is addressable too. The upstream `targetAuthor`
  follows the row direction — the linked account's own number for outgoing rows, the conversation
  peer for incoming direct rows (the quote-resolution precedent). Pending/failed/unknown rows
  carry no protocol identity and answer `MESSAGE_NOT_FOUND`; a group incoming row persists only a
  local sender hash, never the member's number, so its author is not resolvable and the request
  fails with a deterministic `INVALID_REQUEST` instead of a mis-addressed upstream reaction. The
  conversation's kind selects the addressing: `recipient: [peerKey]` for direct chats,
  `groupId: peerKey` for groups (signal-cli jsonRpc `sendReaction` params
  `account`/`emoji`/`remove`/`targetAuthor`/`targetTimestamp`/`recipient`/`groupId`, verified
  against the pinned 0.14.7 distribution).
- Result: `{"status": "sent"}` when signal-cli confirmed the reaction, or `{"status": "unknown"}`
  when the outcome of the mutating upstream call is indeterminate. The `unknown` semantics equal
  every `*_OUTCOME_UNKNOWN` error: the connector never retries automatically; a host may
  explicitly re-send with a fresh requestId and the same `operationId`. `remove=true` removes a
  previously sent reaction (upstream best effort); the emoji must still identify the reaction to
  remove.
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `MESSAGE_NOT_FOUND` (missing or ineligible row), `RUNTIME_NOT_RUNNING`,
  `INVALID_REQUEST` (shape, emoji, or unresolvable group-incoming author), `UPSTREAM_EXITED`, and
  `UPSTREAM_ERROR` for an explicit upstream rejection (unknown target, unregistered recipient,
  reaction not found) — its `retryable=true` is the global mapping, and hosts must not auto-retry
  reactions on it.
- Known behavior boundaries: the local `messages` row itself stays unchanged on a successful
  reaction, but the own reaction is now recorded in `message_events` under the contract 1.27
  actor model (the linked account as actor, no display name) once signal-cli confirms, and a
  `conversation.changed` event is pushed so host-side pills survive the optimistic window
  without a poll; `remove=true` records the removal the same way. Incoming reaction envelopes
  from peers converge through the same store path on receive; nothing is emitted to
  `message.statusChanged` by this method.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account `sendText`/`remoteDelete`/`sendReaction`/`contacts.sync`/`deleteLocalData`
  serialize), the delete drain barrier, and the per-account request budget. Metrics classify it
  as `send`.

### 4.7 messages.attachments.get (contract revision 1.8, 2026-09-03; **retired with 1.20, 2026-09-24**)

> **Retired (contract 1.20).** The chunked media triple (ADR 0004: `messages.attachments.open`
> / `readChunk` / `closeHandle`) replaced the one-shot base64 reader as the only inbound
> attachment channel; the method, its params `$def`, its lane-table entry, and the fixture
> `getAttachment` handler were removed in the same batch. The history below is kept for
> context; `validate_attachment_payload` and the 100 MiB budget constants survive because the
> send path and the chunked channel still enforce them.

API `1.0` evolves in place again (§4.5/§4.6 precedent); the apiVersion handshake binding is
unchanged. The method is additive and advertised through the handshake `capabilities` array —
calling it against an older connector answers `METHOD_NOT_ALLOWED`.

- `messages.attachments.get` returns one attachment that is **already downloaded** into the
  pinned signal-cli data directory as base64. Params: `accountId`, `conversationId`,
  `messageId`, `attachmentId`, `sizeBytes` (all required). `attachmentId` is the upstream
  attachment id from the received message's attachment metadata (the pinned signal-cli jsonRpc
  receive field `id`); `sizeBytes` is the size the caller expects from that metadata.
- PoC downgrade, recorded per the task contract: the streaming design was evaluated and
  **rejected for this revision** — it would thread a second stream through `host.rs` frame
  accounting, pending budgets, and engine shutdown semantics, well past the allowed intrusion.
  The response is `{"attachmentId", "data"}` — `data` is the base64 payload mirroring the
  upstream `getAttachment` jsonRpc response (`JsonAttachmentData` of the pinned 0.14.7
  distribution, verified in source), and `attachmentId` echoes the request so a caller can
  correlate parallel fetches. The connector adds nothing it does not have: no receive-time
  attachment metadata is persisted locally this revision, so there is no `contentType`/
  `filename` to serve. The local `messages` row is never modified and no event is emitted.
- Why the limit is 5 MiB and not the 10 MiB task ceiling: the task explicitly allowed the
  downgrade only up to 10 MiB, but the pinned engine reads upstream stdout lines with an 8 MiB
  limit (`DEFAULT_UPSTREAM_LINE_LIMIT`, `src/engine.rs`) and a longer line faults the shared
  engine with the existing oversized-output semantics, killing every account in the proxy group.
  10 MiB of raw bytes is ~13.7 MiB of base64 — incompatible. The connector therefore enforces
  5 MiB raw (5,242,880 bytes), which encodes to at most 6,990,508 base64 characters, a
  deliberate margin under the line limit. `sizeBytes` is the caller's declared budget, verified
  against the actual decoded size before anything is returned to the host; a mismatch answers
  `INVALID_REQUEST`. The engine line limit remains the hard backstop: the connector persists no
  attachment metadata locally (receive normalization keeps none), so a caller that declares a
  small size for a genuinely larger attachment still trips the upstream fault — a known PoC
  boundary, and one more reason full media enablement stays future work under §6.7.
- Downloading is out of scope and stays out: the connector keeps starting signal-cli with
  `--ignore-attachments` (§6.7), so `getAttachment` is a pure local read of an
  already-downloaded file (`AttachmentStore.retrieveAttachment`, verified in source — no
  upstream fetch, no network call). An attachment signal-cli has not downloaded answers
  `UPSTREAM_ERROR` (the upstream `FileNotFoundException` → `UserErrorException` jsonRpc error),
  and the row in the local history carries no attachment metadata this revision (receive
  normalization keeps none), so the caller learns attachment ids out of band. This is the
  explicit PoC boundary: it proves the bounded token-for-bytes path end to end; enabling
  downloads and streaming remain future work under §6.7.
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `MESSAGE_NOT_FOUND` (missing conversation/message row), `INVALID_REQUEST`
  (shape, oversized `sizeBytes`, or a size mismatch), `RUNTIME_NOT_RUNNING`, `UPSTREAM_EXITED`,
  `UPSTREAM_TIMEOUT`, and `UPSTREAM_ERROR` for the explicit upstream rejection (attachment not
  downloaded, or the upstream call failed) — its `retryable=true` is the global mapping.
- Dispatch: the upstream call is read-only against signal-cli, but the response writes a large
  frame, so the method joins the **read lane** (`conversations.list`/`messages.list`/`getText`
  semantics, `READ_CONCURRENCY`), with the per-account pending budget unchanged. Metrics
  classify it as `read`.

### 4.8 groups.get (contract revision 1.9, 2026-09-03)

Read-only projection of the local contacts cache for one group. Params:
`{accountId, groupKey}` — `groupKey` is the upstream group id (`opaqueId` shape, 1..128 bytes).
The result is `{peerKey, title, memberCount?, syncedAt}`: `title` is the synced group name,
`memberCount` is present when the sync batch captured a member count (`extra` JSON
`memberCount`), and `syncedAt` is the cache row's sync timestamp.

- Data source and staleness: `groups.get` is served from the rows written by `contacts.sync`, The sync path short-circuits
  batches younger than 60 seconds and best-effort sync runs on `link.finish`, so a fresh link
  can read without an explicit sync; beyond that the row is exactly as stale as the last sync.
  The response carries `syncedAt` so the host can judge staleness itself — the connector adds
  no second cache layer.
- Errors (one new code, registered in the schema error enum): `ACCOUNT_NOT_FOUND` for an
  unknown account; `INVALID_REQUEST` for a malformed `groupKey`; `GROUP_NOT_FOUND` when no
  `kind='group'` contacts row exists for `(accountId, groupKey)` — this covers a group the
  account has left (the sync filters `isMember=false`, so departed groups are never cached),
  a peer key that names a contact instead of a group, and a never-synced group. The method
  never calls the upstream: an unsynced cache is a `GROUP_NOT_FOUND`, not a `listGroups` call.
- Dispatch: no upstream call and no event emission; served under the **read lane** together
  with `contacts.list` (no upstream work, but the same bounded concurrency). Metrics classify
  it as `contacts`.
- Local alias is out of scope here: contact titles in the cache are upstream display names;
  the host-side rename method is `contacts.setLocalAlias`, which does not touch this cache
  either (it renames upstream, via `updateContact`).

### 4.9 contacts.setLocalAlias (contract revision 1.10, 2026-09-03)

Rename one contact as the linked account sees it. Params: `{accountId, peerKey, alias,
operationId?}` — `alias` is 1..128 UTF-8 bytes, `operationId` is an optional host correlation
id (validated, never persisted; same semantics as §4.5/§4.6). The upstream call is the
mutating `updateContact` jsonRpc command with parameters `{"account", "recipient", "name"}`:
`recipient` is the peer key as a **single string** (verified against the pinned 0.14.7
`UpdateContactCommand`, which reads it with `ns.getString("recipient")` — passing an array
would throw a ClassCastException and surface as UPSTREAM_ERROR; this corrects the earlier
feasibility note that wrote `recipient: [peerKey]`), and `name` is the new alias.

- Targeting: `peerKey` must resolve to a peer the account already knows — an existing
  `contacts` row with `kind='contact'`, or an existing `direct` conversation peer. Anything
  else (unknown number, a group key, an unsynced contact) answers `INVALID_REQUEST` before
  any upstream call; no new error code is registered for this method.
- Outcome semantics (sendReaction precedent): upstream `Ok` answers `{status: "updated"}`;
  an indeterminate mutating outcome (`EngineError::UnknownOutcome` — engine exit or timeout
  with the call in flight) answers `{status: "unknown"}`. `unknown` is never retried
  automatically by the connector; the host may re-send with a fresh requestId.
- Dispatch: mutating, so the method joins the **send lane** with the per-account mutex and
  the delete barrier (`sendText`/`remoteDelete`/`sendReaction` semantics); same-account
  mutations serialize. Metrics classify it as `send`. No local row is modified — the alias
  lives in the upstream account data and comes back through the next `contacts.sync`, so
  the response status is the only authoritative answer. No event is emitted (the local
  history carries no contact row to transition).

### 4.10 presence.setTypingMessage (contract revision 1.11, 2026-09-03)

Send or stop the typing indicator to one conversation. Params: `{accountId, conversationId,
stop?, operationId?}` — `stop` defaults to `false` (start typing); `operationId` is an
optional host correlation id (validated, never persisted; same semantics as §4.5–§4.9).
Targeting is by `conversationId` only: the conversation row must already exist in the
local history — direct and group conversations are both addressable — and anything else
(missing row, foreign account) answers `CONVERSATION_NOT_FOUND` before any upstream call.

- Upstream call: the mutating `sendTyping` jsonRpc command. Verified against the pinned
  0.14.7 distribution bytecode: `SendTypingCommand.getName()` is `sendTyping`; its dests
  are `recipient` (`nargs="*"`, consumed with `ns.getList("recipient")` — `getList` wraps
  a scalar in a one-element list, so the array form the connector sends follows the
  `sendReaction` precedent), `group-id` (mapped to the camelCase `groupId` JSON key by
  `JsonRpcNamespace`'s dash→camel fallback), and `stop` (a boolean dest read with
  `ns.getBoolean`). `TypingAction` carries START/STOP; the upstream indicator expires
  automatically after roughly 15 seconds, so `stop` is an explicit early clear, not a
  requirement for the indicator to end. The connector always sends the `stop` key
  explicitly (`{account, stop, recipient: [peerKey]}` or `{account, stop, groupId}`) —
  with the key absent `getBoolean` answers null and the connector does not depend on how
  the upstream treats a missing boolean.
- Outcome semantics (sendReaction precedent): upstream `Ok` answers `{status: "sent"}`;
  an indeterminate mutating outcome (`EngineError::UnknownOutcome`) answers
  `{status: "unknown"}`. `unknown` is never retried automatically by the connector; the
  host may re-send with a fresh requestId.
- Dispatch: mutating, so the method joins the **send lane** with the per-account mutex
  and the delete barrier (`sendText`/`remoteDelete`/`sendReaction`/`setLocalAlias`
  semantics); same-account mutations serialize. Metrics classify it as `send`.
- Nothing is written locally and no event is emitted: a typing indicator is ephemeral
  upstream protocol state, not a history row. The inbound direction is out of scope this
  revision — incoming typing events remain a declared `TODO` in the event contract.

### 4.11 messages.sendReceipts (contract revision 1.12, 2026-09-05) — reserved, not wired

Registered intent: `messages.sendReceipts` sends **delivery receipts** for previously
received messages, shape `{accountId, conversationId, timestamps, type, operationId?}`
with `type` deliberately restricted to `"DELIVERY"`. Read receipts are out of scope for
this method by contract: READ receipts leak exactly-when-read and the connector never
sends them. This revision registers the contract only — the method is **not** added to
the schema method enum, so the connector answers `METHOD_NOT_ALLOWED` today — because
the pinned signal-cli 0.14.7 distribution exposes no upstream channel to send a
delivery receipt. Verified against the pinned distribution bytecode:

- The only receipt command is `SendReceiptCommand`, whose jsonRpc name is `sendReceipt`
  (singular). Its `--type` dest carries exactly two choices (`read`, `viewed`), and its
  `handleCommand` dispatches only `Manager.sendReadReceipt`/`Manager.sendViewedReceipt`,
  falling through to `UserErrorException` for anything else. The full jsonRpc command
  table contains no `sendReceipts` (plural) and no delivery-sending command.
- The libsignal layer does define `SignalServiceReceiptMessage$Type.DELIVERY`, but no
  CLI code path reaches it from jsonRpc: `Manager` exposes no `sendDeliveryReceipt`.
- A host-triggered delivery receipt is redundant protocol state to begin with: the
  pinned `IncomingMessageHandler.handleMessage` already constructs
  `SendReceiptAction(recipientId, Type.DELIVERY, timestamp)` for incoming messages, so
  the engine answers deliveries automatically on receipt.

Consequences for the host:

- Delivery receipts need no connector API: they are guaranteed by the engine's inbound
  path, not by an explicit host call.
- The reservation exists so a future upstream that wires `sendDeliveryReceipt` can add
  the method without reshaping the contract. Registering a method that could only fail
  upstream (`sendReceipt` with an unsupported type, answering `UPSTREAM_ERROR` on every
  call) was rejected: a permanently-failing lane is worse than the explicit
  not-implemented `METHOD_NOT_ALLOWED` answer.

### 4.12 messages.attachments.send (contract revision 1.13, 2026-09-22)

API `1.0` evolves in place again (§4.5–§4.7 precedent); the apiVersion handshake binding is
unchanged. The method is additive and advertised through the handshake `capabilities` array —
calling it against an older connector answers `METHOD_NOT_ALLOWED`. It follows the
`messages.sendText` (§4.2) implementation shape end to end: pending row → upstream mutating
call → `complete_send_success` / `complete_send_unknown` settlement.

- `messages.attachments.send` sends one attachment (optionally with a text caption) to one
  conversation. Params: `accountId`, `conversationId` **or** `kind`+`peerKey`(+`peerTitle`)
  addressing (exactly one form, `sendText` semantics), `clientRequestId` (idempotency),
  `dataBase64`, `sizeBytes` (declared raw budget), optional `filename` (1–128 bytes;
  caller-supplied display name, rejected outright on path separators or control
  characters — never sanitized), optional `contentType`
  (1–128 chars, `type/subtype` shape), optional `text` caption (existing `validate_text`
  bounds). The base64 payload is **decoded by the connector and re-encoded as an RFC 2397
  data URI** — upstream jsonRpc `send` accepts `attachments` entries as file paths or data
  URIs, and `AttachmentHelper` (pinned 0.14.7 distribution, verified in bytecode and source)
  decodes data URIs itself, uploads via the CDN path, and manages its own temp file lifetime.
  The connector therefore never persists attachment bytes and never passes caller-controlled
  file paths upstream (signal-cli itself rejects data-directory paths — same boundary).
- Why not file paths: the host→connector boundary passes bytes, not paths (§8 discipline);
  a connector-managed temp file would create a third cleanup owner and a crash window, and
  upstream already implements the data-URI decode with its own bounded temp handling.
- Why the limit stays 5 MiB raw (5,242,880 bytes): symmetric with `messages.attachments.get`
  (§4.7). The inbound host frame limit rises with this revision — `DEFAULT_HOST_FRAME_LIMIT`
  1 MiB → 16 MiB (`lib.rs`) — because 5 MiB raw encodes to at most 6,990,508 base64 chars
  (~6.99 MB) plus the JSON envelope; 16 MiB matches the desktop-side
  `DEFAULT_MAX_CONNECTOR_FRAME_BYTES` so the whole chain narrows at the same gate. The 8 MiB
  upstream **output** line limit (`DEFAULT_UPSTREAM_LINE_LIMIT`) is not in the send path: the
  `send` response carries only ids/timestamps, and the data URI travels connector→upstream on
  an inbound line for which signal-cli sets no length cap (`BufferedReader.readLine`,
  verified in v0.14.7 source). The frame limit raise is deliberate and reviewed: one host
  connection may hold one ≤16 MiB line buffer, bounded by the existing per-connection model;
  the desktop client is the only local peer (private Unix socket / named pipe, peer
  handshake-authenticated), so the exposure is the same trust domain as before.
- Result: the sent `MessageRecord` (same projection as `sendText`) on upstream confirmation,
  or `SEND_OUTCOME_UNKNOWN` when the mutating outcome is indeterminate — identical settlement
  and no-auto-retry discipline as every other mutating send (§4.2). The pending row's `text`
  is the caption (may be empty). The pending and sent rows carry the metadata-only attachment
  descriptor (the inbound wire shape, contract 1.15) so the desktop renders the outgoing
  attachment from its first tick, and the conversation preview falls back to the filename
  when the caption is empty — never a blank preview for an attachment send.
- Error mapping reuses existing codes only (zero new codes): `ACCOUNT_NOT_FOUND`,
  `CONVERSATION_NOT_FOUND`, `INVALID_REQUEST` (shape, base64 decode failure, size mismatch,
  bounds violations), `RUNTIME_NOT_RUNNING`, `UPSTREAM_EXITED`, `UPSTREAM_TIMEOUT`,
  `UPSTREAM_ERROR` for an explicit upstream rejection (`AttachmentInvalidException` family) —
  its `retryable=true` is the global mapping, no auto-retry.
- Dispatch inherits the mutating infrastructure unchanged: the write lane with the per-account
  mutex (same-account sends serialize), and the per-account request budget. Metrics classify
  it as `send`.

### 4.13 Inbound control plane: quote / reaction / remote delete / typing / edit / attachment metadata (contract revision 1.15, 2026-09-23)

API `1.0` evolves in place (§4.5–§4.12 precedent). Receive normalization stops dropping the
five per-message control surfaces the official clients and signal-cli both carry, and attaches
bounded inbound attachment metadata to message rows. Everything stays metadata-only: bytes are
still never downloaded or persisted (§6.7 media boundary unchanged).

Inbound shapes (pinned signal-cli `MessageEnvelope` json surface, cross-checked against the
official v0.14.8 JSON schemas):

- **quote** (`dataMessage.quote` / `syncMessage.sentMessage.quote`): `{id, author, text}` —
  the quoted message's upstream timestamp, author number, and bounded text preview. Stored on
  the message row; the host resolves display through its own history.
- **reaction** (`dataMessage.reaction` / `syncMessage.sentMessage.reaction`):
  `{emoji, targetAuthor, targetSentTimestamp, isRemove}` — recorded as a per-conversation
  event (`message_events` table), not a message row, mirroring the protocol shape. The host
  receives a `conversation.changed` event and re-reads events with the conversation.
- **remoteDelete** (`dataMessage.remoteDelete` / `syncMessage.sentMessage.remoteDelete`):
  `{timestamp}` — the target upstream timestamp authored by the envelope sender. The matching
  outgoing row (our own multi-device delete) or incoming row (peer delete) in the same
  conversation is marked remote-deleted locally (no upstream call); the host receives
  `message.statusChanged` with the new status.
- **typing** (`typingMessage`): `{action, groupId}` — START/STOP is ephemeral state only,
  never persisted. Emitted to the host as a new `conversation.typing` host event; a fixed
  in-memory rate limiter (one notification per account+peer per second, bounded map, oldest
  entry evicted) keeps a typing storm from flooding the event lane. The host owns display
  timeout.
- **edit** (`editMessage` / `syncMessage.sentMessage.editMessage`):
  `{targetSentTimestamp, dataMessage}` — the new body replaces the target row's body when the
  row exists in the same conversation and the editor matches the row's sender identity; the
  row's `edited_at` marks it edited. Missing/mismatched targets are dropped without a local
  row; the upserted event still notifies the host.
- **attachment metadata** (`dataMessage.attachments[]` / `syncMessage.sentMessage.attachments[]`):
  `{id, contentType, filename, size, width, height, isVoiceNote}` — bounded to 32 entries per
  message, each `id` ≤ 128 chars, `filename` ≤ 128 bytes, `contentType` ≤ 64 chars. Persisted
  as a JSON column on the message row. Bytes are still never downloaded (`--ignore-attachments`
  unchanged): `messages.attachments.get` keeps answering `UPSTREAM_ERROR` for these ids until
  a dedicated bounded download PoC exists (§6.7). `messages.attachments.send` (§4.12) also
  records its own single descriptor on the sent row at confirm time, so the sender's own
  bubble shows the attachment across restarts.

Consequences for the host: the `MessageRecord` projection gains optional `quote`, `attachments`,
`remoteDeleted`, and `editedAt` fields (absent on rows without them), and the host event surface
gains `conversation.typing` (ephemeral) alongside the existing `message.upserted` /
`message.statusChanged` / `conversation.changed` events.

### 4.14 messages.edit (contract revision 1.15, 2026-09-23; upstream face migrated 1.105)

`messages.edit` edits one previously sent message upstream: params `accountId`,
`conversationId`, `messageId`, `text`, `clientRequestId`. It resolves the target exactly like
`messages.remoteDelete` (addressable outgoing row `sent`/`delivered`/`read` = upstream protocol identity),
then dispatches the engine's native `editMessage` wire method with
`targetTimestamp: <upstream timestamp>` plus the same target addressing (`recipient` array /
group master key `groupId`) — since contract 1.105 (§4.86); the original 1.15 face dispatched
the signal-cli-era `send` + `editTimestamp`, which the kt-engine reads as a brand-new message.
Official clients allow a 24 h edit window; the connector adds no local window — a late edit
answers the upstream rejection (`UPSTREAM_ERROR`), keeping the connector's rule set minimal.

Settlement mirrors `messages.sendText`: on confirmed success the row's body is replaced and
`edited_at` set (status unchanged) and the method answers the updated `MessageRecord`; an
indeterminate outcome answers `SEND_OUTCOME_UNKNOWN` with the local row unchanged. No
auto-retry.

### 4.15 Attachment budget to official scale + reaction aggregate projection (contract revision 1.16, 2026-09-23)

Two alignment changes with the official clients' behavior, both host-visible without new
methods or events.

**Attachment size budget.** `messages.attachments.send` lifts the deliberate PoC ceiling from
5 MiB to the official 100 MiB (104857600 bytes, the same budget Signal Desktop enforces). The
four framing constants move together so one attachment never overruns its container:

| Constant | Value | Role |
| --- | --- | --- |
| `MAX_ATTACHMENT_BYTES` (service) | 104857600 | declared + validated attachment payload |
| `DEFAULT_HOST_FRAME_LIMIT` (lib) | 167772160 (160 MiB) | one inbound host frame |
| `DEFAULT_UPSTREAM_LINE_LIMIT` (lib) | 167772160 (160 MiB) | one signal-cli JSON-RPC line |
| `MAX_PENDING_HOST_BYTES` (host) | 184549376 (176 MiB) | in-flight frame accumulation pool |

The base64 expansion of a 100 MiB attachment (~134 MiB) plus its JSON wrapper fits the 160 MiB
frame/line limits; the host pool holds one full frame plus overhead. `build_attachment_data_uri`
now sizes its buffer from the actual base64 length instead of preallocating the maximum. The
schema's `sizeBytes` maximums and the `attachments.get`/`attachments.send` descriptions state
the same budget. Upstream acceptance of very large payloads still depends on the pinned
signal-cli reader (§6.7 boundary unchanged: bytes are never persisted connector-side).

**Reaction pills in the message projection.** `MessageRecord` gains `reactions`:
`[{emoji, count, mine}]`, aggregated from `message_events` rows (`removed=0`, grouped by emoji,
`mine` marks the linked account's own reaction, oldest first). The field is always serialized —
an empty array, never absent — so a host that merges upserted records into cached state
replaces the whole list and never keeps a stale pill. Read paths (`messages.list`,
`messages.get`) and every `message.upserted` event path (send settlement, edits in both
directions) attach the aggregates; reaction add/remove itself notifies through
`conversation.changed` only (§4.13 shape), and the host refreshes the open conversation from
that event.

The schema also gains the `conversation.typing` event entry (§4.13 shipped the event but
omitted it from the schema enum; data `{accountId, conversationId, action: START|STOP}`).

### 4.16 Retire messages.attachments.get (contract revision 1.20, 2026-09-24)

The desktop contract (kt-desktop `contracts/signal-host-adapter.md` 1.20) removes
`getAttachmentSessionMessage` / `messages.attachments.get`: the ADR 0004 chunked triple
(§4.15-era media PoC, contract revision 1.17) has been the default inbound-attachment channel
since 1.17 and the one-shot base64 path only added a second code path to validate (whole
payload through one host frame, no chunk budget). Removal is symmetric across both repos:

- host: dispatch branch, `MessagesGetAttachmentParams`, service `prepare_get_attachment`,
  supervisor `get_attachment` (the only upstream `getAttachment` call), registry router,
  lane-table entry, metrics label, schema method enum + `messagesAttachmentsGetParams` `$def`
  + if/then binding, and the fixture's `getAttachment` handler.
- kept on purpose: `validate_attachment_payload` / `MAX_ATTACHMENT_BASE64_CHARS` (send path),
  `sanitize_attachment_id` (open path), and all budget constants — the chunked channel and
  the send path still enforce them.

Failure-shape coverage moves with the replacement: NOT_FOUND-before-upstream addressing and
sanitize/path-traversal rejection are asserted on `messages.attachments.open`
(`open_media_handle`, integration + unit tests), and the upstream base64 passthrough test was
deleted with the method.

### 4.17 Device-unlink detection and account recovery (contract revision 1.21, 2026-09-24)

The desktop contract (kt-desktop `contracts/signal-host-adapter.md` 1.21) adds a joint
recovery path for the case where the phone unlinking a linked device leaves stale local
state on both sides. Previously the engine kept pinging a dead link forever, the supervisor
kept restarting it (ping failures looked like any other engine failure), and the desktop
surfaced a permanent `ACCOUNT_BOUND` wall with no way forward from the UI.

Connector behavior, all inside the existing process/watchdog boundary (no new IPC method):

- upstream error classification: engine responses carrying
  `AuthorizationFailedException` / `Authorization failed` map to
  `EngineError::Unauthorized` instead of a generic upstream error.
  Post-mortem widening (2026-10-05, field incident): after the first
  authorization failure signal-cli persists `registered=false`
  (`AccountHelper.java:119`) and every later account-scoped command fails
  LOCALLY with `UserErrorException("User <account> is not registered.")`
  (`App.java:342` wrapping `NotRegisteredException`; JSON-RPC
  `USER_ERROR = -1`) before any network call — the 0-17 ms instant-failure
  shape. That text previously missed the classification, so a phone-side
  unlink churned watchdog restarts forever and the desktop only saw generic
  `UPSTREAM_ERROR`. The classification now also matches the strict
  `User … is not registered.` prefix/suffix pair; the recipient-level
  `The user <id> is not registered.` (unblock/updateContact/sendPoll*
  family) deliberately does NOT match (starts "The user ") so a bad contact
  can never flip the account state. Covered by the
  `.fixture-unregistered-user-status` marker and two engine unit tests plus
  a `supervisor_watchdog` integration test. Boundary note (contract 1.99,
  §4.80): this matcher's match set is frozen — the send-chain transport
  anchors classify in a standalone pure function
  (`upstream_error_send_class`) and deliberately do not extend it.
- attribution: because `send`-family failures arrive without a caller context, the engine
  attaches the owning account (from the request params' `account` field) to the failure and
  emits a new internal `AccountUnauthorized { account }` event; requests with no account
  attribution stay unclassified.
- durable state: `store.mark_account_device_unlinked` moves the account to a new
  `device_unlinked` static state (guarded against clobbering an in-flight `unlinking`);
  the service turns the store result into host-side `account.changed` events so the
  desktop sees the transition.
- watchdog: an `Unauthorized` ping result resets the failure counter and does NOT restart
  the engine — restarting cannot fix a phone-side unlink and only burns the episode
  budget. The account-unlink watch loop (spawned in `Supervisor::new`, aborted on drop)
  consumes `AccountUnauthorized` events; the registry router deliberately ignores them
  (no-op arm) so harnesses without a registry still exercise the real path.
- wire surface: error enum gains `ACCOUNT_UNLINKED` (`retryable: false`). The upstream
  classification, the store state, and the wire code are covered by unit tests, the
  `fake-signal-cli` `.fixture-auth-failed-user-status` marker, and a
  `supervisor_watchdog` integration test asserting the account flips state without a
  restart.

Desktop-side obligations (same revision): the `device_unlinked` state enters the wire
state enum; linking over a dead owner binding auto-cleans (unbind + local-data purge only
when the collision is a different account id) and proceeds; deleting a session purges its
connector session via the host adapter.

### 4.18 Conversation summary last-message kind (contract revision 1.23, 2026-09-28)

The desktop contract (kt-desktop `contracts/signal-host-adapter.md` 1.23) adds an optional
`lastMessageKind` to conversation summaries so the desktop list renders the official
attachment-noun preview ("📷 Photo" / "🎥 Video" / "🎤 Voice message" / "📎 File") without
reverse-engineering it out of the preview string.

- derivation is read-time only, per summary row: the newest message
  (`ORDER BY sent_at DESC, id DESC LIMIT 1`, covered by the existing
  `messages_conversation_sent_at` index) decides. An attachment-only ending — the row has
  attachments and no non-empty body text — yields the first attachment's kind:
  `image/*` → `image`, `video/*` → `video`, `audio/*` or the voice-note flag → `audio`,
  anything else → `file`. A text/caption ending, a system row, or an empty conversation
  yields nothing: the field is omitted and the desktop falls back to `lastMessagePreview`.
  The caption wins over the noun — exactly the official list-preview behavior
  (official source: a body text takes precedence over the attachment type noun).
- wire shape: `lastMessageKind?: 'image' | 'video' | 'audio' | 'file'` on
  `ConversationSummary`, `skip_serializing_if = "Option::is_none"` like
  `lastMessagePreview`. Nothing new is persisted; the contract 1.15
  preview/filename fallback stays authoritative for the text itself.
- cost: one extra indexed LIMIT-1 query per summary row per page read (local SQLite,
  desktop-scale page sizes). No write-path change, no cache, no new IPC method.

### 4.19 Inbound peer receipts (contract revision 1.24, 2026-10-01)

`envelope.receiptMessage` — the delivery/read confirmation a peer sends back for our
outgoing rows — stops being skipped and routes through the inbound control plane (§4.13
precedent). Shape (pinned signal-cli `JsonReceiptMessage`): `{timestamp: [...], when,
isDelivery, isRead, isViewed}`.

- tier selection: `isViewed` > `isRead` > isDelivery-default. The desktop status ladder is
  `pending → sent → delivered → read`, so `viewed` maps onto `read` on the wire; there is
  no fourth tier to surface.
- confirmed timestamps are bounded (256, de-duplicated, truncation on overflow) and resolve
  to outgoing rows in the envelope sender's direct conversation by upstream `sent_at`.
- upgrade is monotonic and per row: `pending|sent → delivered → read`; a lower tier never
  downgrades a higher one, and incoming/terminal/system rows are never touched. Only real
  transitions emit `message.statusChanged` (one per moved row) — replays are silent.
- conversation scoping: receipts carry no `groupId`, so only direct conversations resolve
  (envelope sender as peer). Group-sent rows stay untouched for now — a declared boundary,
  not a silent gap.
- nothing new is persisted: receipt state lives in the message rows' `status` column, so
  history reads and restarts see the last tier for free. Offline-period receipts (peer
  confirmed while the connector was down) are lost with the envelope and surface on the
  next real receipt — the same eventual consistency the official clients accept.
- no new IPC method, no schema-event addition: the host learns through the existing
  `message.statusChanged` event and its existing status projection.

### 4.20 Inbound rich bodies: previews / mentions / textStyles / viewOnce (contract revision 1.25, 2026-10-01)

The four rich-body fields a peer's `dataMessage` (and the multi-device `syncMessage.sentMessage`
mirror) carries stop being dropped and flow to the host on the message row. Shapes are the pinned
signal-cli 0.14.8 JSON models, verified by decompiling `JsonPreview` / `JsonMention` /
`JsonTextStyle` from the distribution jar:

- `previews: [{url, title?, description?, image?}]` — bounded to 4 cards; url capped at 2048
  chars, title/description at 512 UTF-8 bytes. `image` reuses the §4.13 metadata-only attachment
  descriptor (id/contentType/filename/size/width/height); the connector never fetches image bytes.
- `mentions: [{number|uuid, name?, start, length}]` — bounded to 64 ranges; author resolves
  number-first, name capped at 128 bytes. Ranges index into the received body text.
- `textStyles: [{style, start, length}]` — bounded to 64 ranges; only `BOLD` `ITALIC`
  `STRIKETHROUGH` `MONOSPACE` `SPOILER` pass validation, `NONE` and unknown values drop
  individually.
- `viewOnce: true` — persisted only when true; plain messages carry nothing.

Bounds follow §4.13: entries past a cap drop, oversized strings truncate, malformed entries drop
individually — a hostile payload can never fail the message itself. An all-default result
collapses to absent, so rows without rich data are byte-identical to pre-1.25 rows.

Persistence: one additive nullable `messages.rich_json` TEXT column (schema 8 → 9) holding the
packed `NormalizedRich` JSON, read back flattened onto the wire MessageRecord as
`previews` / `mentions` / `textStyles` / `viewOnce` (absent keys stay absent on the wire). Rich
fields never change after receive — an edit replaces the body but keeps the original ranges
(the official client re-renders edited bodies the same way until a new body arrives with fresh
ranges). Outgoing rows our client sends stay plain text; the desktop does not compose rich bodies.

No new IPC method or event: the host learns through the existing `message.upserted` payload and
history reads. The desktop renders previews as link cards, mentions as highlighted ranges, the
four styles, and a view-once badge; its boundaries are declared in the desktop contract 1.25.

### 4.21 Outbound link previews on messages.sendText (contract revision 1.26, 2026-10-01)

`messages.sendText` gains an optional `previews` array; only the first entry is used because the
pinned signal-cli builds `List.of(one)` preview per send (verified by decompiling `SendCommand` /
`ManagerImpl` from the distribution jar). The entry shape is `{url, title, description?,
imageDataUri?}`, and every constraint is enforced deterministically before the pending row exists:

- `url`: trimmed, non-empty, ≤ 2048 bytes, absolute http(s), and **must appear in the message
  text** — signal-cli's own requirement ("the same url must also appear in the message body")
  surfaced here as a local `INVALID_REQUEST` instead of a late upstream send error.
- `title`: non-empty, ≤ 1024 bytes. `description`: optional, ≤ 4096 bytes.
- `imageDataUri`: optional, `data:image/*` RFC 2397 data URI only, ≤ 1.5M chars (~1.1 MiB
  binary). Bytes stay in memory and pass straight to upstream — the same pattern as
  `messages.attachments.send` (upstream `AttachmentHelper.uploadAttachment` is data-URI aware,
  verified from bytecode). **No caller-controlled file path ever reaches upstream.**

Validation passes project 1:1 onto the upstream JSON-RPC keys `previewUrl` / `previewTitle` /
`previewDescription` / `previewImage` (`JsonRpcNamespace` maps dash-separated option names to
camelCase — verified from bytecode). The connector never composes or synthesizes preview content.

### 4.22 Conversation-summary author/reaction metadata + per-actor reaction detail (contract revision 1.27, 2026-10-02)

Two read-side capability additions that close the desktop list/interactions gaps the v8.31 parity
audit filed against the wire ("wire 缺作者/反应元数据"): the conversation list cannot render the
official group-author prefix, reaction emoji prefix, or send-state direction on the last message,
and the ReactionViewer ("who reacted") has no per-author data — the 1.16 pill projection is
aggregated only. No new IPC method or event; both surfaces evolve in place.

**Author-name capture (shared foundation).** The receive envelope's `sourceName` — the peer's
profile/contact label as the linked account sees it, already bounded to 64 chars by the engine —
is now persisted at receive time on two additive nullable columns (schema 9 → 10):
`messages.sender_name` for incoming message rows and `message_events.actor_name` for inbound
reaction events. Outgoing rows and own multi-device echoes store nothing (the author is the
account itself). Pre-1.27 rows are never backfilled: the name a past envelope carried was never
stored and must not be invented. `MessageRecord` projects the column as `senderName`
(skip-when-absent), so the desktop can already name group senders on the canvas.

**Conversation summary fields.** `ConversationSummary` gains three optional fields derived at
read time from the newest row (`ORDER BY sent_at DESC, id DESC LIMIT 1`, the existing
`messages_conversation_sent_at` index; one extra indexed LIMIT-1 reaction query per row, bounded
to 8 distinct emoji):

- `lastMessageDirection?: 'outgoing' | 'incoming'` — absent when the conversation has no rows or
  ends on a system row, the same endings the official preview shows without send state or author.
  `outgoing` marks the linked account as the author: the client renders its own localized self
  label ("You"), the connector never ships locale-dependent strings.
- `lastMessageAuthorName?: string` — the newest incoming author's captured display name; absent
  for outgoing (self), system endings, and rows without a captured name.
- `lastMessageReactions?: string[]` — distinct active reaction emoji on the newest row, oldest
  reaction first (the same order the message pills use); absent when none. A reaction on an older
  row never leaks into the summary, and removal drops the emoji.

All three use `skip_serializing_if`, so rows without the data are byte-identical to pre-1.27
summaries and a host that merges summaries replaces the fields wholesale.

**Per-actor reaction detail.** The storage was already per-actor (`message_events` keys
`(conversation, target_timestamp, actor_id)` with `emoji`/`removed`/`updated_at`, one row per
actor per target); only the wire aggregated it. `MessageReactionSummary` gains `actors` (always
serialized, like `reactions` itself, so host-side pill merges stay total and a removal clears the
actor it removed):

```text
reactions: [{
  emoji: string,          // unchanged (1.16)
  count: number,          // unchanged (1.16); == actors.length
  mine: boolean,          // unchanged (1.16)
  actors: [{              // new in 1.27, newest reaction first (official ReactionViewer order)
    self: boolean,        // true = the linked account's own reaction
    name?: string,        // peer display name captured from the reaction envelope's sourceName
    reactedAt: number     // local wall-clock ms the reaction was last recorded
  }]
}]
```

Removal semantics: a removed reaction (`isRemove` upsert) is excluded from `count` and `actors`
entirely — removal is the actor's disappearance, exactly like the aggregated count it already
followed; re-reacting flips the row back with a fresh `reactedAt`. Reads are bounded
deterministically: at most 1024 newest active reaction rows per conversation read and 64 actors
per emoji, so a pathological store cannot inflate a response. `actorId` (the connector-internal
sender hash) deliberately stays off the wire — it is not resolvable by the host and carries no
rendering value beyond `self` + `name`.

### 4.23 Conversation-summary last-message send state (contract revision 1.28, 2026-10-02)

The last of the three list-preview gaps the v8.31 parity audit filed against the wire
(audit-B `b-row-status`, P1): the official list renders the send-state icon on the last message
— only when that message is outgoing (`sending` 4s spinner / `sent` check / `delivered`
double-check / `read`·`viewed` solid double-check / `error` red exclamation) — and the summary
carried no field to drive it. Pure read-side addition, the 1.27 pattern: no new IPC method, no
event, no store-schema change (the `status` column has been persisted at receive/send time since
the 1.15 era; nothing is backfilled because nothing needs to be).

`ConversationSummary` gains one optional field derived in the same single newest-row read as the
1.23/1.27 projections (`conversation_last_message_meta`, the existing
`messages_conversation_sent_at` index, zero extra queries):

- `lastMessageStatus?: 'pending' | 'sent' | 'delivered' | 'read' | 'failed'` — present only when
  the newest row is `outgoing`, and then exactly that row's `status` value: the same vocabulary
  `MessageRecord.status` projects, so the list icon and the bubble icon can never disagree. The
  desktop maps the tiers onto its existing official projection (`pending` → sending spinner,
  `sent` → check, `delivered`/`read` → double-checks, `failed` → red exclamation); the connector
  ships no icon or locale-dependent string.

Boundaries, all explicit: incoming and system endings and empty conversations expose nothing
(the official icon exists only on outgoing endings); an outgoing row that ended
`remote-deleted` or `unknown` exposes no status either — the official icon set has no
representation for a deleted last message, so the row degrades to the same no-icon rendering an
incoming ending gets while `lastMessageDirection` still marks it self-authored. There is no
`paused` tier (the connector has no pausable send pipeline) and no `error` tier — send failures
already land as `failed` rows via the existing failure path, which the summary reflects for
free. Receipt upgrades (§4.19) move the summary field for free on the next read: it is a
projection of the row, never a cached copy.

The field uses `skip_serializing_if`, so endings without a send state are byte-identical to
pre-1.28 summaries. Feasibility note recorded for the desktop parity backlog (verified against
the pinned signal-cli command registry, all 61 JSON-RPC methods enumerated from source): mute,
archive, forward, and full-text search have no upstream JSON-RPC support — conversation-level
mute does not exist upstream at all, `listContacts[].isArchived` is a read-only legacy-contact
projection with no write API, and there is no `forward` or `search` method; any desktop-side
mute/archive/forward/search can therefore only be connector-local state or a local
approximation (never cross-device synced), which is a separate decision, not part of this
revision.

### 4.24 Unread @mentions: row marker + conversation badge (contract revision 1.29, 2026-10-02)

The official list renders an @-mention badge next to a conversation's unread counter when the
unread messages @mention the account (Signal-Desktop `unreadMentionCount`), and highlights the
mentioned range as "@you" on the bubble. Neither was representable on the wire: the desktop
cannot decide self-ness itself because the account address it sees (`maskedAddress`) is masked,
and mention ranges carry the *mentioned peer's* identity, not the reader's. Two additive
read/write surfaces close the gap; the store schema moves 10 → 11.

**Row marker.** `MessageRecord` gains `mentionsSelf?: boolean` (skip-when-false, so plain rows
are byte-identical to pre-1.29 rows): at receive, the ingest compares every normalized mention
author (§4.20 `mentions[].author`, resolved number-first by the engine) against the linked
account's own number and persists the verdict on the row in the same insert that stores the
rich body. The comparison is **number-based by necessity**: the pinned upstream jsonRpc surface
exposes the account only as `listAccounts: [{number}]` (`JsonAccount(number)` in the pinned
source) — the account UUID is not queryable — so a mention author carrying only a UUID cannot
be attributed to self. That is this revision's recorded boundary; it under-counts only when
upstream fails to resolve a self-mention to the number, not a false positive. The verdict is
receive-time and never recomputed or backfilled: pre-1.29 rows carry no flag, edits never move
it (the counter counts receipt-time mentions, like the official receipt-time increment), and
the client renders "@you" from `mentionsSelf` + the existing §4.20 ranges without needing the
real address.

**Conversation badge.** `conversations` gains `unread_mentions` (schema 11), a write-side
counter moving in lockstep with `unread_count`: the same receive transaction bumps both (an
incoming row that mentions self and contributes unread bumps both or neither — a crash can
never split the pair), and the same open-chat clear (`conversations.markRead`) zeroes both.
`ConversationSummary` gains `unreadMentions?: number` (skip-when-zero, pre-1.29 hosts read
byte-identical summaries). There is deliberately no account-level mention aggregate — the
official badge is per-conversation too — and no summary read cost: the counter is maintained
at write time, mirroring how `unread_count` already works.

Boundaries, all explicit: only incoming rows can mention self (outgoing/system rows never set
the flag and never touch the counter); a remote-deleted row stays unread, so both counters
keep counting it — the same boundary `unread_count` already has (the official client
decrements on delete; this connector's delete path does not adjust unread, and the @ badge
follows its sibling rather than inventing its own rule); retention pruning removes rows
without touching either counter. The desktop renders the badge from `unreadMentions` alone and
never diffs message lists to maintain it, so receipt upgrades, edits, and reactions cannot
drift it.

### 4.25 messages.search: account-wide local full-text search (contract revision 1.30, 2026-10-02)

The official client searches messages as-you-type (`Signal-Desktop` `MessageSearch`), but it does
so entirely over its **local** database — upstream Signal offers no server-side search endpoint,
and the pinned `signal-cli` jsonRpc surface has none either (§4.23). The connector's store is the
only searchable corpus, so `messages.search` is a bounded read over it: `params` are `accountId`,
`query` (required), `cursor?`, `limit` (1..=200, same clamps as `messages.list`). The result is
`Page<MessageRecord>` — identical row shape to `messages.list`, with reaction aggregates attached
— so the client renders a hit exactly like a thread row.

Semantics, all explicit: the query is trimmed; an empty or whitespace-only query answers an empty
page (the client treats it as "no query" rather than an error); a query over 128 characters is
rejected `INVALID_REQUEST` (the wire schema caps the raw string at 4096 so an oversized paste gets
the semantic error, not a schema violation). Matching is a `LIKE '%term%'` over stored bodies with
`ESCAPE '\'` — `%`, `_`, and `\` in the query are literal. Ordering is `sent_at DESC, id DESC`
across **every conversation in the account**; pagination rides a dedicated `s1:accountId:sentAt:id`
cursor carrying its own sort key (a page still resolves after retention removes a row it pointed
at) and bound to the account alone, since results span conversations. Rows whose body was never
stored (attachment-only or unretrievable) simply never match — `LIKE` over `NULL` is false.

Recorded boundaries: SQLite `LIKE` folds case only for **ASCII** — non-ASCII terms (CJK among
them) match byte-exactly, so a CJK query is case-sensitive by construction (which is also what
the official desktop ships on SQLite, minus its FTS5 index). There is no FTS5 index, no
tokenization, and no relevance ranking this revision: substring over `body` only. Search never
touches upstream, never mutates state, and rides the READ lane (`read` metrics class, not
account-scoped-mutating). `PHASE2_CAPABILITIES` grows by one entry, advertised at handshake.

### 4.26 messages.retryText: same-row resend of a failed send (contract revision 1.31, 2026-10-03)

The official client offers "resend" on a message that definitively failed to send, and the resent
message **replaces** the failed one — the history never grows a second row for the same logical
message. The connector's `clientRequestId` idempotency made the naive desktop-side retry wrong: a
retry that minted a new id inserted a second pending row while the old `failed` row stayed in the
store, so every refresh rendered both (a failed bubble plus its resent twin). `messages.retryText`
makes the retry a first-class connector operation instead: `params` are `accountId`,
`conversationId`, `clientRequestId` (the idempotency key of the ORIGINAL send — persisted rows
always carry it, so both an in-session optimistic row and a reloaded store row address the same
target). The service resolves the row via `message_by_client_request`, guards it, rearms it
in place, and re-dispatches the upstream send rebuilt from the persisted record; the pending id
never changes, so settlement (success / `SEND_OUTCOME_UNKNOWN` / definite failure) completes the
SAME row and the history keeps exactly one row per logical message.

Guards, all explicit: the row must be an outgoing **text** row with a stored `clientRequestId` in
the terminal state `failed` — a missing row answers `MESSAGE_NOT_FOUND`, `pending` answers
`RETRY_IN_FLIGHT` (double-click or a concurrent rearm; the store-level
`WHERE status='failed'` rearm makes the transition atomic), `unknown` answers `RETRY_NOT_ALLOWED`
(its wire outcome was never settled, so a blind resend could double-deliver — the standing rule
"an unknown send outcome must not be retried automatically" extends to user-initiated retries),
incoming/system rows and rows without a clientRequestId answer `INVALID_REQUEST`, and an empty
body answers `INVALID_REQUEST` (attachment retry needs the attachments path and is out of scope
this revision). Rebuilt upstream params carry the persisted body and quote (`quoteTimestamp` /
`quoteAuthor` re-resolved at retry time); staged link previews are not persisted with the row and
therefore do not ride a retry — the wire message goes out plain-text when its original preview
staging is gone, matching the composer-less nature of a retry.

Recorded boundaries: the rearm is one store transaction (`failed -> pending` only), so a status
push that already settled the row between guard and rearm cannot be overwritten; the send lane
(`Lane::Send`, mutating, metrics class `send`) serializes the retry with ordinary sends of the
same account; the row stays `pending` while the upstream call is in flight, and a definite
upstream error flips it straight back to `failed` (the desktop's retry affordance reappears).
There is no automatic retry anywhere in the path — the connector retries nothing by itself; only
an explicit `messages.retryText` may re-dispatch a failed send, and only a definite failure may
be so re-dispatched.

### 4.27 Peer-receipt timestamps + prior-body edit history (contract revision 1.32, 2026-10-03)

Two host-visible data additions, both read-only consequences of events the engine already
delivers — the connector previously projected them away.

**(a) Delivery/read receipt timestamps.** The desktop's message-info page (official
MessageDetail) shows *when* a peer receipt arrived; the wire `message.statusChanged` event
carried only the new `status`, so the desktop could show the ladder but never a timeline.
`ControlReceive::Receipt` now carries `when` — the receipt envelope's own `timestamp` (when the
peer sent the receipt), falling back to connector wall-clock when absent. The monotonic
`upgrade_outgoing_receipts` write stamps `delivered_at` / `read_at` (additive nullable columns,
store schema 12) on the rows the receipt actually moves: first stamp wins (`COALESCE` — a
replayed or later receipt never re-stamps), and no history row is backfilled (receipts only
ever arrive live). The `message.statusChanged` event gains optional `deliveredAt` / `readAt`
(copy of the just-written stamps), and the `MessageRecord` row projection carries the same
optional fields so a reloaded window renders the timeline without re-deriving it. Boundary:
the local "view thread marks it read" badge write is store-local only and stamps nothing —
no peer receipt, no timestamp.

**(b) Prior-body edit history.** The official client keeps every prior body of an edited
message (EditHistoryMessagesModal, newest first). The connector edits overwrote the body in
place, so prior text was unrecoverable. Every accepted edit — inbound peer edit, host-initiated
`messages.edit`, multi-device mirror edit — now snapshots the PRIOR body into a `message_edits`
table (schema 12) inside the same transaction as the overwrite: `{messageId, body, bodyBytes,
editedAt}` where `editedAt` is the replacement time. Bounds: one entry ≤ 4096 bytes
(UTF-8-safe truncation, same budget as the host text preview), at most 20 entries per message
(oldest pruned), `editedAt` ascending = chronological order; the desktop renders newest-first
like the official unshift. The `MessageRecord` row projection gains optional `edits`
(absent on rows never edited), so list reads, search hits and every `message.upserted` carry
the full history without a new method. The table cascades with its message row (foreign key ON
DELETE CASCADE, `foreign_keys=ON` already enforced) and retention passes remove it with the row
— the store never grows edit orphans.

### 4.28 Message pin/unpin + group admin delete (contract revision 1.33, 2026-10-04)

Three additive mutating methods plus three inbound projections, all carrying official protocol
fields the engine's stack already speaks — `DataMessage.pinMessage = 27`, `unpinMessage = 28`,
`adminDelete = 29` (Signal-Desktop v8.31.0-alpha.1 `protos/SignalService.proto`, verified;
behavior aligned with official `SendMessage.preload.ts` / `processDataMessage.preload.ts`).
Official semantics: one pinned message per conversation, `pinDurationSeconds != null` means a
timed pin, absent/null means forever; `unpinMessage` carries only the target identity;
`adminDelete` is group-only (a group admin removing someone else's message; direct chats have no
admin concept).

- `messages.sendPinMessage`: params `accountId`, `conversationId`, `messageId` (required; the
  reaction addressing model §4.6 verbatim), optional `pinDurationSeconds` (u32; absent = forever,
  out-of-range fails closed with `INVALID_REQUEST`), optional `operationId` (shape-validated,
  never persisted). Engine call: `sendPinMessage` with `targetAuthor` per row direction (own
  number for outgoing rows, peer for incoming direct rows, unresolvable group-incoming rows keep
  the §4.6 `INVALID_REQUEST` answer) — the engine resolves it to the 16-byte
  `targetAuthorAciBinary` the wire requires. Groups map to the engine `groupId` form.
- `messages.sendUnpinMessage`: same addressing, no duration. Engine call: `sendUnpinMessage`.
- `messages.sendAdminDelete`: same addressing, group conversations only (`INVALID_REQUEST`
  otherwise). Engine call: `sendAdminDelete`. Target eligibility against admin state is not
  pre-checked — the server is the authority and rejects non-admin attempts; the connector
  surfaces the upstream rejection (official behavior: send and show the failure).
- Result follows the reaction precedent exactly: `{"status": "sent"}` / `{"status": "unknown"}`,
  no automatic retries, same write-lane mutex and delete-drain barrier. Error mapping reuses
  existing codes only. Handshake `capabilities` gains `pin-messages` and `admin-delete`
  (contract-gated discovery, §4.5 precedent).
- Inbound projections: `receive` envelopes now carry optional `pinMessage`
  (`{targetAuthor, targetSentTimestamp, pinDurationSeconds?}` — absent `pinDurationSeconds` =
  forever), `unpinMessage` (`{targetAuthor, targetSentTimestamp}`), and `adminDelete`
  (`{targetAuthor, targetSentTimestamp}`) keys on `dataMessage`, with the reaction targetAuthor
  resolution (E.164 on contacts hit, ACI string otherwise). The connector persists the derived
  per-conversation pinned state (at most one entry; a newer pin replaces the older one; expiry
  from `pinDurationSeconds` is connector-clock computed) and surfaces it on the conversation
  summary as optional `pinnedMessage: {messageId, targetAuthor, targetSentTimestamp,
  pinnedAt, expiresAt?}` so a reloaded window renders the pin bar without replaying events;
  `unpinMessage` and `adminDelete` clear/update that state and persist as row-level markers
  (`adminDeleted` flag renders the official "deleted by admin" tombstone; the row body stays
  for the audit window, retention prunes normally). Store schema 13 (additive columns + one
  table), no data migration.
- Known boundaries: pins are conversation-scoped (the official model) — the backup-archive
  `ChatItem.PinDetails` surface is engine-side import metadata (§6.6) and does not feed this
  state; phone clients older than the official pin rollout ignore the unknown field (proto
  forward-compatibility), so peers without support simply never show the pin.

### 4.29 Outbound read/viewed receipts + send rich-face + group receive parity (contract revision 1.34, 2026-10-04)

The outbound receipt face, the send-side mention face, and the group-receive parity note. All
upstream JSON-RPC extensions below are provided by `kt-signal-engine` (the ADR-0005 replacement
line, behavior pinned to official Signal-Desktop `sendReceipts.preload.ts` /
`SendMessage.preload.ts`); against stock `signal-cli 0.14.8` the new methods fail upstream and
surface through the existing error mapping — a declared swap boundary, not a silent gap.

- `messages.markRead` / `messages.markViewed`: send-lane, account-scoped mutating (reaction
  precedent §4.6 write-lane mutex). Params: `accountId`, `conversationId`, optional `messageIds`
  (bounded 512, incoming rows only — outgoing and system rows are skipped; absent = every
  receipt-eligible incoming row of the conversation, ascending `sent_at`, bounded 512 —
  §4.119: absent mode selects only rows newer than seven days, and the viewed faces select
  only rows with no `viewed_at` stamp yet). The connector does not
  persist our own read state (the desktop owns its unread state), so absent mode may re-receipt
  rows the desktop already marked — duplicates are protocol-idempotent for the peer (the §4.119
  seven-day window bounds that replay surface; the viewed face's `viewed_at` first-stamp
  excludes already-played rows entirely). Rows whose
  author cannot be resolved from local data (group rows carrying only the sender hash, §4.28)
  are skipped from the fan-out, not an error. The connector groups the selected rows by author
  and calls the engine `sendReadReceipt` / `sendViewedReceipt` once per author (single-author
  recipient + that author's timestamps; the engine chunks 100 per envelope, official CHUNK_SIZE).
  Result follows the reaction precedent: `{"status": "sent"}` / `{"status": "unknown"}` — a
  receipt send failure never fails the request, and unknown outcomes are never retried
  (AGENTS.md). Zero eligible rows is a trivial `{"status": "sent"}` no-op. Handshake
  `capabilities` gains `send-receipts`. (Revision 1.94, §4.77: `messages.markViewed`
  additionally first-stamps the selected incoming rows' local `viewedAt` before the
  fan-out — the played marker is the request's own local effect, lands even when the group
  list is empty, and survives an upstream failure; `messages.markRead` keeps its
  no-local-write discipline unchanged.) (§4.119: the stopgap record for the selection
  filters and the supervisor failure debounce — connector-internal, no wire change.)
- Auto delivery receipts: for every incoming envelope the connector best-effort calls the engine
  `sendDeliveryReceipt` (author, [sent_at]) after the row lands — official receiving-client
  behavior, no user setting, no retry, fully silent (failures change nothing).
- `messages.sendText` gains optional `mentions: [{number, start, length}]` (bounded 64, the §4.20
  receive caps mirrored; `number` resolves contacts-first, ACI passthrough; offsets are UTF-16
  code units, the official BodyRange semantics — the caller indexes its own body). Engine side
  converts to `BodyRange.MentionAci`. Bounds violations drop the entry; a fully malformed array
  fails closed `INVALID_REQUEST`.
- Upstream rich-face parity notes (no connector change): `quoteTimestamp`/`quoteAuthor`
  (contract 1.13) and `previewUrl`/`previewTitle`/`previewDescription` (§4.21) now take real
  effect on the engine wire face — quote `text` is enriched engine-side from the engine's own
  store (official/`signal-cli` behavior; when the quoted body is unknown the quote rides without
  text, the official degrade path), and `previewImage` (same data-URI shape as `attachments`)
  uploads with the attachment batch and lands as the official `Preview.image` (engine
  2026-10-04; before that face imageless previews rode protocol-valid).
- Group receive parity: the engine now projects the signal-cli-shaped `groupInfo {groupId}` on
  inbound group `dataMessage` / `sentMessage` (master-key base64, the exact shape
  `data_message_group_id` already consumes), so group receives land through the unchanged
  connector pipeline — the earlier "engine skips groups" note is void. Group conversation
  skeletons remain progressive-fill (first group message); direct-chat skeletons are covered by
  the engine's contacts sync on link/restart (its receive pipeline runs one
  `request_contacts` round per account, warn-only, 60s one-shot — no connector change).

### 4.30 Outbound voice notes on messages.attachments.send (contract revision 1.35, 2026-10-04)

`messages.attachments.send` (§4.12) gains an optional `voiceNote: boolean` (default false). When
true the connector's `build_attachment_data_uri` appends the `;voice=true` data-URI parameter and
the engine maps it to the official `AttachmentPointer.flags = VOICE_MESSAGE` — the exact wire
shape the official desktop recorder emits (`audioRecorder.preload.ts`: attachment with
`flags=VOICE_MESSAGE`, contentType `audio/mpeg`, no separate message body; receiving clients'
`isVoiceMessage` judges by the flag first). Validation: `voiceNote` requires `contentType` to be
`audio/*` (the official recorder only produces audio; anything else is `INVALID_REQUEST` before
the pending row exists). The sent row's attachment descriptor records `isVoiceNote: true`, so
the sender's own bubble renders the voice UI across restarts.

Receive side is already contract-complete: the inbound attachment descriptor has carried
`isVoiceNote` since contract 1.15 and the engine projects it from the pointer flags — zero
receive-plane change. Waveform rendering is a desktop concern (the official client derives it
from the decoded audio; no protocol surface). No new method, no schema change, no capability
entry — `attachments.send` callers discover the face from the updated schema description.

### 4.31 Stickers: messages.sendSticker + receive projection (contract revision 1.35, 2026-10-04)

Official send face (`sendStickerMessage`): `DataMessage.sticker {packId, packKey, stickerId,
emoji, data: AttachmentPointer}` with `body` undefined, no regular attachments, mutually
exclusive with text/attachments/previews/mentions, quote allowed to co-ride. The pinned
signal-cli JSON-RPC has no sticker method (sticker faces are CLI-command-only), so this is an
engine-extension method mirrored 1:1 by the connector.

**`messages.sendSticker`** (new host method, Send lane, mutating, §4.12 settlement discipline:
pending row → upstream → `complete_send_success` / `complete_send_unknown`, no auto-retry,
advertised through handshake `capabilities` as `send-sticker`): params `accountId`,
`conversationId` **or** `kind`+`peerKey` addressing (exactly one form), `clientRequestId`,
`packId` (hex, even-length, 1–64 chars — official pack ids serialize hex), `packKey` (standard
base64, 1–128 chars — official `Bytes.fromBase64` convention), `stickerId` (u32), `emoji?`
(≤ 32 chars), `image` `{dataBase64, sizeBytes, contentType, width?, height?}` — the same budget
and validation family as §4.12 (≤ 100 MiB declared+validated, path-separator/control-char
rejection n/a here since no filename) with one sticker-specific rule: `contentType` must be
`image/*` (the official client sniffs image MIME and falls back to `image/webp`; it refuses
`video/*` and `text/*`). The connector composes the engine `send` call with the `sticker`
object (packId/packKey passthrough after shape validation, image re-encoded data URI,
width/height passthrough — official `AttachmentPointer` display metadata) and an empty body.
The sent row's descriptor records the sticker image attachment plus a `sticker` metadata field
(packId/packKey/stickerId/emoji) so the sender's bubble renders as a sticker across restarts.

**Receive projection**: the engine projects inbound `DataMessage.sticker` as
`sticker {packId(hex), packKey(base64), stickerId, emoji?, data}` (official serialization
conventions). The connector normalizes it onto the row: the sticker metadata persists as an
additive JSON column (`messages.sticker_json`, schema 13 → 14) and `MessageRecord` gains an
optional `sticker` field (`skip_serializing_if` absent — pre-1.35 rows stay byte-identical).
The `data` pointer rides the existing metadata-only attachment descriptor list (contract 1.15
shape, bounded 32) so byte fetch goes through the existing media channel — the connector never
downloads sticker bytes (§6.7 boundary unchanged). Reactions/replies/remote-delete on sticker
rows behave exactly like attachment rows (the sticker is body-less metadata, not a special row
kind). A sticker on a message that also carries `pinMessage`/`unpinMessage`/`adminDelete`
control surfaces keeps working — the control plane is orthogonal (§4.28).

Bounds failures are deterministic `INVALID_REQUEST` before the pending row exists; unknown
mutating outcomes answer `SEND_OUTCOME_UNKNOWN` and are never retried.

### 4.32 Sticker pack browsing: getStickerPackManifest + getStickerImage (contract revision 1.36, 2026-10-04)

The browse face of contract 1.36: two additive Read-lane host methods through which the desktop
fetches a sticker pack's manifest and one sticker's decrypted bytes on demand. Neither method
takes an `accountId` — pack browsing is anonymous CDN traffic in the official client, entirely
outside any linked account's session — and neither reads or writes connector state: no row, no
cache, no event, no persistence. Both are engine-extension methods (the pinned signal-cli
JSON-RPC has no sticker-pack face) forwarded 1:1 to the engine, which performs all network and
crypto work.

Official behavior reference (file-level): `ts/textsecure/WebAPI.preload.ts` (anonymous CDN GET
of `manifest.proto`), `ts/Crypto.node.ts` (HKDF `"Sticker Pack"` → AES-256-CBC + HMAC-SHA256
verify/decrypt), `ts/types/Stickers.preload.ts` (manifest decode, `isPackIdValid`, and the 200
sticker display window). The engine owns the whole pipeline: anonymous CDN GET, digest
verification, decryption, proto decode. The connector never touches key material beyond passing
the pack key through as a parameter — the key exists on the wire only as the base64 the host
supplied, is never persisted, never logged, and never leaves the request scope (the §6.7
boundary's crypto wording, kept verbatim: bytes and keys both stay engine-side).

- `getStickerPackManifest`: params `packId` (exactly 32 lowercase-or-uppercase hex characters),
  `packKey` (standard base64 that decodes to exactly 32 bytes). Result `{title, author, cover,
  stickers, stickerCount}` where `cover` is `{id, emoji?, contentType?} | null` and `stickers`
  is `[{id, emoji?, contentType?}]`. Bounds: manifest entries are capped at 1024 — the official
  decoder truncates over-long proto lists, the connector mirrors that truncation semantics
  rather than rejecting (official behavior); `title`/`author` are ≤ 256 characters (KT bounds —
  the official decoder applies no length validation; the connector keeps the early
  `INVALID_REQUEST` fail-closed guard instead of passing absurd strings through). The 32-hex
  packId is deliberately stricter than the 1.35 sendSticker receive projection (even-length hex
  ≤ 64): the browse face aligns with the official `isPackIdValid`, which accepts exactly 32 hex
  characters for pack ids minted by the official sticker creators.
- `getStickerImage`: params `packId` (same 32-hex rule), `packKey` (same 32-byte rule),
  `stickerId` (u32). Result `{dataBase64, contentType, size}` where `dataBase64` is the
  decrypted standard-base64 image (≤ 400 KB encoded — the official sticker CDN caps uploads at
  300 KB, and 4 × ceil(307200 / 3) = 409600 characters covers the base64 expansion with margin),
  `contentType` is the manifest/sniffed image media type, `size` the decoded byte count. The
  decoded bytes exist only inside the engine for the lifetime of the response; the connector
  persists nothing and never decodes or re-encodes beyond the pass-through.

Lane classification: both methods are **Read lane, non-mutating** — CDN fetches mutate no
upstream state, so no account mutex, no delete barrier, no pending-row settlement, and a
timeout answers the ordinary retryable `UPSTREAM_TIMEOUT` (unlike mutating calls, a read that
did not answer provably took no effect). Error mapping: engine error codes surface through the
structured connector codes — `STICKER_PACK_KEY_INVALID` / `STICKER_PACK_FETCH_FAILED` /
`STICKER_PACK_MALFORMED` / `STICKER_IMAGE_TOO_LARGE` map to the connector's
`STICKER_PACK_KEY_INVALID` / `STICKER_PACK_FETCH_FAILED` / `STICKER_PACK_MALFORMED` /
`STICKER_IMAGE_TOO_LARGE` (`retryable=false` for key-invalid and too-large,
`retryable=true` for fetch-failed and malformed — a malformed upstream proto may be fixed by a
refetch; an invalid key never succeeds on retry), engine-down answers the existing
`RUNTIME_NOT_RUNNING`, and an old engine without the method answers the existing
`CAPABILITY_UNAVAILABLE` (same "engine does not support this face" mapping the §4.29
engine-extension family uses; desktops gate on the handshake `capabilities` tag
`sticker-pack-browse`, contract-gated discovery per §4.5 precedent).

### 4.33 Conversation pin sync: getPinnedConversations + setConversationPinned (contract revision 1.36, 2026-10-04)

The conversation-level pin face of contract 1.36: two host methods that read and write the
official conversation-pinning state. Source of truth is the Signal Storage Service
`AccountRecord.pinnedConversations` (official mechanism, `ts/services/storage.preload.ts`):
the upstream stores an ordered list of conversation references, array order = pin order (no
timestamps — pinnedAt ordering is a desktop render concern derived from list position), and the
storage service write path is the same one the official desktop uses when the user pins or
unpins a chat.

- `getPinnedConversations`: params `accountId`. Result `{pinned:
  [{conversationId, kind}]}` with `kind` the three-value enum `contact` | `group` |
  `legacyGroup` (GroupsV1), order exactly the cloud order. Read lane, non-mutating.
- `setConversationPinned`: params `accountId`, `conversationId`, `kind` (same enum), `pinned`
  (boolean, required). Result: the same `{pinned: [...]}` projection read back after the
  upstream write, so the host sees the post-write cloud state — including the server-applied
  ordering — instead of a local guess. Send lane, account-scoped mutating (the reaction-class
  write-lane mutex and delete drain barrier verbatim; an unknown mutating outcome answers
  `SEND_OUTCOME_UNKNOWN` and is never retried, per AGENTS.md).

Connector responsibilities are strictly bounded: forward, bounds-check, and resolve the
conversation identity and account context. `conversationId` resolves through the existing
ladder (§4.4 wire order: account, then conversation — `CONVERSATION_NOT_FOUND` before any
upstream call) so the connector maps the host's opaque id to the upstream conversation
reference the engine needs, mirroring the conversationId resolution every other method uses;
`kind` is validated against the three-value enum; `accountId` resolves the owning proxy group
and engine through the normal routing. The connector does not interpret pin semantics and adds
no second ordering.

Bounds (early `INVALID_REQUEST`, deterministic, before any upstream call): `conversationId` ≤
256 characters (a conversation reference bound, wider than the local opaqueId family because
the host may address a conversation it has not yet materialized locally), `kind` enum as above,
`pinned` required boolean, and the returned `pinned` list is capped at 128 entries (KT bound —
the official clients apply no explicit cap; the connector truncates beyond 128 to keep one
host frame bounded and records the truncation in the §5 boundary notes). Engine errors pass
through structurally: `CONVERSATION_NOT_RESOLVED` maps to the connector's
`CONVERSATION_NOT_RESOLVED` (`retryable=false` — the cloud reference does not resolve against
the account's storage state), storage-service failures surface under the engine's
`STORAGE_*` codes verbatim (`retryable=true`; the connector neither swallows nor invents
storage codes — the engine owns their final set), and engine-down / method-absent follow the
§4.32 mapping (`RUNTIME_NOT_RUNNING` / `CAPABILITY_UNAVAILABLE`). Handshake `capabilities`
gains `conversation-pin-sync`.

Orthogonality and live-push boundaries: message-level pins (contract 1.33 `pinMessage` /
`unpinMessage`, §4.28 — at most one pinned *message* per conversation, projected onto the
conversation summary) and conversation-level pins (this face — ordered list of pinned
*conversations* in the account record) are two unrelated protocol concepts that merely share
the word; neither feeds the other, and the §4.28 `pinnedMessage` summary field is untouched by
this face. Live push is out of scope for this face: the connector does not subscribe to storage-service
pin writes, and the engine projects no inbound pin event; the host pulls with
`getPinnedConversations` at startup/refresh (poll-on-open replaces push). The connector
persists no pinned list — the cloud record plus the desktop's own cache is the whole story; no
new on-disk face, no store schema change.

### 4.34 Sticker pack sync: stickerPacks.getSyncs + stickerPacks.setSync (contract revision 1.37, 2026-10-04)

The account-scoped sticker-pack install face of contract 1.37: two host methods that read and
write the official per-account sticker-pack sync records. Source of truth is the Signal Storage
Service `StickerPackRecord` set (official mechanism): an installed pack is the record
`packId + packKey + position`, an uninstall is the tombstone `packId + deletedAtTimestamp`
(the engine stamps its own clock), and a re-install/update inserts the new key while the old
key's record is deleted — the connector sees only the projected record list and adds no
semantics of its own. Unlike the §4.32 browse face these records are account state: they live
in the account's storage session and ride the account's engine.

- `stickerPacks.getSyncs`: params `accountId`. Result `{packs: [{packId, packKey|null,
  position|null, deletedAtTimestampMs|null}]}` — installed packs carry key and position,
  uninstalled packs the tombstone timestamp. Read lane, non-mutating.
- `stickerPacks.setSync`: params `accountId`, `packId`, `packKey` (optional), `installed`
  (boolean, required), `position` (optional u32). Result: the same `{packs: [...]}` projection
  read back after the upstream write, so the host sees the post-write cloud state instead of a
  local echo. Send lane, account-scoped mutating (the reaction-class write-lane mutex and
  delete drain barrier verbatim; an unknown mutating outcome answers `SEND_OUTCOME_UNKNOWN`
  and is never retried, per AGENTS.md).

Engine wire (frozen shapes, kt-signal-engine): `getStickerPackSyncs {account}` and
`setStickerPackSync {account, packId, packKey|null, installed, position|null}`. Engine
semantics: `installed=true` must carry a valid pack key (exactly 32 bytes); `installed=false`
ignores `packKey`/`position` entirely and writes the tombstone with the engine's clock — the
connector forwards null for both in that case, so the wire shape is deterministic regardless of
what the caller supplied.

Connector responsibilities are strictly bounded: forward, bounds-check, and resolve the account
context (the account maps to its owning proxy group and engine through the normal routing; the
upstream key is the resolved `signal_account`, never the host's opaque id). The connector does
not interpret install semantics, keeps no pack list, and persists nothing — the cloud record
plus the desktop's own cache is the whole story; no new on-disk face, no store schema change.

Bounds (early `INVALID_REQUEST`, deterministic, before any upstream call): `packId` exactly 32
hex characters (the §4.32 browse-face shape, `isPackIdValid`-aligned), `packKey` when present
standard base64 decoding to exactly 32 bytes (44 padded characters), `installed` required
boolean, `position` within the u32 range (out-of-range values fail at params deserialization),
and the returned `packs` list is capped at 256 entries (KT bound — the official clients apply
no explicit cap; the connector truncates beyond 256 to keep one host frame bounded and records
the truncation in the §5 boundary notes). Entries missing the identity field (`packId`) drop;
the optional fields project leniently beside it. Engine errors pass through structurally: the
`STORAGE_*` family (`STORAGE_UNAVAILABLE` / `STORAGE_READ_FAILED` / `STORAGE_WRITE_FAILED`)
maps verbatim with `retryable=true` — the allowlist and schema already declare these codes, the
§4.4 classification tests stay total over the same set, and no new codes are introduced.
Engine-down and method-absent follow the §4.32 mapping (`RUNTIME_NOT_RUNNING` /
`CAPABILITY_UNAVAILABLE`). Handshake `capabilities` gains `sticker-pack-sync`.

### 4.35 Sticker reply: sendSticker quote coexistence (contract revision 1.37, 2026-10-04)

`messages.sendSticker` gains the optional `quoteMessageId` parameter: a sticker message that
quotes one earlier message in the same conversation — the official `StagedStickerReply` shape
(one upstream `send` carrying the sticker object and the quote identity together). The receive
side needs no new face: an incoming sticker with a quote already lands as the ordinary sticker
projection beside the ordinary quote projection (both engine normalizer fields have been
independent since 1.15/1.31), so this section is send-side only.

Resolution reuses the `dispatch_send` ladder verbatim (§4.4 wire order, no new errors): the
quote targets a row in the same conversation; an unknown target answers `MESSAGE_NOT_FOUND`;
a target whose Signal author is not established (unaddressable outgoing state, group incoming
row, system row) answers the deterministic `INVALID_REQUEST`. The upstream keys are
`quoteTimestamp` (the quoted row's Signal timestamp) and `quoteAuthor` at the top level of the
`send` params beside the `sticker` object — the same wire keys the text face uses, because the
quote belongs to the send, not to the sticker. Absent `quoteMessageId` is exactly the 1.35
behavior (an unquoted sticker).

Ordering discipline (a quote is part of send validation): resolution runs after the
idempotency short-circuit and addressing resolution and before the pending row is inserted — a
rejected quote leaves nothing behind, so the same `clientRequestId` stays a fresh
(re-validated) request, while a replay of an already-inserted row keeps returning the existing
row without re-resolving. The pending row records `quoteMessageId`; lane, account-scoped
mutating classification, and §4.12 settlement (upstream-timestamp completion,
`SEND_OUTCOME_UNKNOWN` with no automatic retry, failure marks the same row) are the 1.35
`sendSticker` discipline verbatim. Bounds: `quoteMessageId` is the `opaqueId` family, the same
schema bound the text face carries.

### 4.36 Outbound view-once: attachments.send `viewOnce` + messages.sendViewOnceOpen (contract revision 1.38, 2026-10-04)

The send-side view-once face: a view-once media message our client composes, plus the bare
open-notification passthrough the official `sendViewOnceOpenSync` mirrors. Upstream extensions
are provided by `kt-signal-engine` (behavior pinned to official Signal-Desktop view-once send
semantics); against stock `signal-cli 0.14.8` the new keys and method fail upstream and surface
through the existing error mapping — the declared swap boundary of §4.29.

- `messages.attachments.send` (§4.12) gains an optional `viewOnce: boolean` (default false).
  Constraints, all deterministic `INVALID_REQUEST` before the pending row exists: `viewOnce`
  requires `contentType` to be `image/*` or `video/*` (the official view-once faces are photos
  and videos only), requires `caption` to be empty, forbids `quoteMessageId`, and forbids
  `voiceNote` (audio is never view-once; the single-attachment structure of §4.12 already makes
  the combination unambiguous). A validated send projects a pending row whose rich record carries
  `viewOnce: true`, so the sender's own bubble renders the view-once UI across restarts, and the
  upstream `send` params carry `"viewOnce": true` at the top level beside the attachment data URI
  (the key is absent when the flag is false — the wire shape stays byte-identical to the 1.13
  send for every existing caller). Lane, account-scoped mutating classification, and §4.12
  settlement are unchanged.
- `messages.sendViewOnceOpen`: params `accountId`, `senderAci` (`opaqueId` family), `timestamp`
  (u64 ms). Send lane, account-scoped mutating (the reaction-class write-lane mutex and delete
  drain barrier verbatim). The connector resolves the account (the upstream key is the resolved
  `signal_account`, never the host's opaque id) and forwards `{account, senderAci, timestamp}`
  to the engine `sendViewOnceOpen`. Result follows the receipt precedent (§4.29):
  `{"status": "sent"}` / `{"status": "unknown"}` — any upstream failure (engine down, method
  absent, unknown outcome) degrades to `"unknown"` and is never retried (AGENTS.md). Bounds
  (early `INVALID_REQUEST`, before any upstream call): `senderAci` non-empty, bounded by the
  `opaqueId` length family; `timestamp` is u64 (out-of-range fails at params deserialization).
  `ACCOUNT_NOT_FOUND` keeps its existing meaning when the account does not resolve.

Handshake `capabilities` gains `view-once`. This face is a bare passthrough: the connector adds
no queue, no dedup, and no persistence of its own. The desktop-facing orchestration that wraps
it lives in §4.37.

### 4.37 View-once burn: viewOnceOpen consumption + erasure + messages.markViewOnceOpened (contract revision 1.38, 2026-10-04)

The receive-side view-once face: consuming a peer's open notification, erasing the viewed bytes,
and the desktop-facing trigger that orchestrates both legs of the official view-once flow. The
engine (kt-signal-engine) already projects the official `syncMessage.viewOnceOpen
{senderAci, timestamp}` — the sender's client notifies every linked device when the recipient
viewed the message — and re-delivers the same sync envelope once per resync cycle, so exactly-once
is the connector's job.

- Receive projection: the engine's sync branch parses `viewOnceOpen`; a missing `senderAci`
  drops the envelope; an absent `timestamp` normalizes to 0 (the "unknown" marker). The
  connector receives it as a new control receive kind `viewOnceOpen` (direction "incoming",
  no session routing) and intercepts it in the control pipeline after account resolution but
  before the `(kind, peer_key)` conversation routing — the sync envelope's own source is our
  account, so generic routing would misroute it into a self-conversation.
- Target ladder: `timestamp == 0` warns and drops. The candidate peer-key list is the literal
  `senderAci` plus every stored identity that maps to it (a new bounded `peer_identities` table
  captures the ACI→peer-key mapping from ingest from 1.38 forward — one row per ACI,
  primary-keyed `(account_id, aci)`, written only when an incoming envelope's `sourceUuid`
  differs from the stored peer key; rows before 1.38 rely on the ladder's fallbacks). The
  candidate query joins conversations, caps at 16 rows (incoming-conversation rows first), and
  takes the first row whose rich record carries `viewOnce: true`. No match warns and drops —
  warn-only, no state, no error: the official clients stage open syncs until the message
  arrives, this connector deliberately does not (no ViewOnceOpenSyncs staging; a sync that
  arrives out of order is dropped and the sender's resync cycle re-delivers it).
- Idempotency and the event: the first transition marks the row opened, clears the body bytes
  (the row's projection downgrades to a viewed placeholder — the bytes are no longer available),
  deletes the media file and preview under the connector-owned attachment directories, closes
  any open media handle for the message, and emits exactly one new host event
  `message.viewOnceOpened {accountId, conversationId, messageId, openedAt}`. Duplicate sync
  envelopes (the engine's pinned-rev resync delivers the same envelope twice) find no
  transition and emit nothing. The event is a local-viewed notice, deliberately not folded into
  `message.statusChanged` (that carries peer receipts, not our own view state).
- `messages.markViewOnceOpened`: params `accountId`, `conversationId`, `messageId`. The
  connector resolves the target through the standard ladder (missing row `MESSAGE_NOT_FOUND`).
  A row that is not a view-once message (`rich.viewOnce` absent — valid or invalid-flagged rows
  both qualify) answers deterministic `INVALID_REQUEST`. The burn runs first: on a fresh
  transition it erases the row bytes and media exactly as above and emits
  `message.viewOnceOpened`; on a replay (already opened) it is a trivial `{"status": "sent"}`
  no-op with no event and no upstream fan-out. After a successful burn the connector
  best-effort sends the official notification pair, in the official order, under the §4.29
  receipt discipline (any failure degrades `{"status": "unknown"}`, never retried; the burn and
  the event are unaffected): for an incoming row with a resolvable author, engine
  `sendViewedReceipt {recipient, timestamps: [sentAt]}`; then engine `sendViewOnceOpen
  {senderAci, timestamp}` where `senderAci` is the author for incoming rows and our own account
  for outgoing rows (the engine resolves the registered number to the own ACI). A group row
  whose author cannot be resolved skips the receipt leg (the §4.28 boundary) but still burns.
- Orchestration ownership (the §4.4 division, recorded): `messages.markViewOnceOpened` is the
  desktop's single trigger point; the connector internally orchestrates the VIEWED receipt and
  the ViewOnceOpen notification in the official order. The desktop does not need to call
  `messages.markViewed` or `messages.sendViewOnceOpen` itself — a stray call is harmless
  (protocol-idempotent) but sits outside the desktop flow; the bare faces of §4.29/§4.36 remain
  available.
- Media reads: `attachments.open` on a view-once row whose `viewOnceOpenedAt` is set answers
  `UPSTREAM_ERROR` with the exact wording of the not-downloaded path, so an opened view-once
  attachment is indistinguishable from one the engine never fetched — no new error surface
  leaks the state transition. `viewOnceInvalid: true` rows (the metadata-only §4.20 marker) are
  included in byte erasure the same way.

No store schema-version bump: the `peer_identities` table is additive (`CREATE TABLE IF NOT
EXISTS` runs on every open), and the row-level erasure reuses the existing rich/body columns.
Bounds stay structural: candidate cap 16, peer-key list bounded by the contacts table, media
deletion reuses the §4.12-best-effort discipline (missing files are success).

### 4.38 Link-time history import: backup5 archive consumption + history.importStatus (contract revision 1.39, 2026-10-04)

The connector side of kt-signal-engine ADR 0005 S3: consuming the NDJSON export the engine's S2
stage streams after a `backup5` link, importing it into the store under the same identity and
bounded-batch discipline as live traffic, and exposing an honest status face for the desktop's
S4 import UI. The engine face is file-based by design (engine 0eedc8c: "the consumer decides by
file presence") — no engine JSON-RPC method or event changes, so the connector consumes a
documented data-directory artifact instead of an RPC surface.

- Engine face (consumption contract, complementing the layout already recorded in §5): after a
  `backup5` link the engine best-effort downloads and decrypts the transfer archive and streams
  `history-import.ndjson` into the account's store directory (`<engine data-dir>/accounts/<dir>/`,
  with `<dir>` recorded per number in the engine's `engine-state.json` registry, compat §1).
  Export failure leaves the file absent; an engine crash mid-export can leave a partial file
  (no completion marker exists on the v1 face). The connector parses the one-JSON-object-per-line
  records — `recipient {id, kind: contact|group|self|other, name, aci?, e164?, masterKey?}`,
  `chat {id, recipientId}`, `message {chatId, authorId, dateSent, direction, text?, attachments?,
  quote?, remoteDeleted?, pinnedAtTimestamp?, ...}` — tolerantly: malformed lines, unknown types,
  and over-long lines are counted and skipped, never fatal. The recipient-level service identity
  fields (`aci`, `e164`, `masterKey`) are required for conversation attribution and are NOT yet
  projected by the engine's S2 export (kt-signal-engine gap, registered for the engine-side S3
  task): until the engine lands them, every import run completes with all chats skip-counted
  (`skippedChats`), which the status face reports honestly. The connector consumes the fields
  when present, so the engine addition needs no further connector change.
- Decision — independent status method, not `link.finish` fields: the engine's archive download
  starts in the background when `finishLink` returns (long-poll plus CDN transfer, minutes) and
  is best-effort, so `link.finish` cannot carry the import outcome without lying or blocking;
  `link.finish`'s result is the stable account summary with its own non-retryable outcome
  discipline (`LINK_OUTCOME_UNKNOWN`), while the import is an idempotent, retryable background
  task with a different lifecycle. The official desktop has the same shape: linking completes
  first, the history import runs behind the main UI. The status method gives the S4 UI the
  pending → running → completed/failed progression it needs; there is deliberately no new event
  in 1.39 — the desktop polls `history.importStatus` while the state is `running` (the UI
  requirement is one number ticking, not low-latency delivery).
- `history.importStatus` params `{accountId}`; result `{accountId, state, ...}` with state
  `unavailable` (`reason: "engine-mode"` under signal-cli JVM/native, which has no backup5
  capability at all; `reason: "no-archive"` when the link-time wait window has expired with no
  archive produced — the engine's best-effort failure modes are indistinguishable from "the
  phone had nothing to transfer" and are reported as such, never guessed), `pending` (linked,
  inside the wait window, archive not on disk yet), `running` / `completed` / `failed` (from the
  persisted import row, which carries `importedMessages`, `skippedLines`, `skippedChats`,
  `skippedMessages`, `attempts`, `errorClass`, `startedAt`, `updatedAt`). Unknown account:
  `ACCOUNT_NOT_FOUND`. The method is read-lane, read metrics class; no new error codes.
  (Contract 1.54, §4.51, extends the `running` state with the optional `progress` object and
  adds the `history.importRetry` manual face — that section owns the widened shape. Contract
  1.56, attachv2 above, widens the import payload itself with the attachment pointer faces;
  the status/progress/retry faces are untouched by it.)
- Store: one `history_imports` row per account (schema version 15, FK cascade with the account;
  the import's attachment pointer faces ride `messages.attachment_locators`, schema 18,
  contract 1.56 — see the attachv2 bullet above),
  plus the bounded import writer. Message rows are written with exactly the live-receive
  identity — the message id is the same `signal-message-v2` stable hash over
  `(account, conversation, direction, sent_at, sender)`, `sent_at` being the Signal timestamp
  the archive carries (`dateSent`) — so `INSERT OR IGNORE` deduplicates against rows that live
  traffic already wrote (or will write): re-running an import, or a later live envelope for the
  same message, lands on the same identity and inserts nothing. Chats attach through
  `ensure_conversation`, so a contacts-sync skeleton (§6.5) is filled in place — no second
  conversation row; the title keeps the §6.5 upgrade policy.
- Attachment pointer faces (contract 1.56, attachv2, 2026-10-06; engine counterpart recorded in
  kt-signal-engine `docs/jsonrpc-compat.md` §5): the transfer archive carries no media bytes —
  recoverability rides the frame's `FilePointer.LocatorInfo` pointer face (key / transit
  cdnKey+cdnNumber / integrity digest|plaintextHash; official anchor
  `convertFilePointerToAttachment`, `ts/services/backups/util/filePointers.preload.ts:51-168`,
  and the four-piece transit download gate `isDownloadableFromTransitTier`,
  `ts/util/Attachment.std.ts:806-825`), which the engine's v1 export discarded. The pinned
  engine now projects a `locator` object per attachment row (plus width/height/caption/blurHash/
  incrementalMac+chunkSize/clientUuid), and this connector consumes it ingest-side only:
  faces land in a new internal `messages.attachment_locators` JSON column (schema version 18,
  additive nullable, never backfilled) index-aligned with the `attachments` descriptors —
  a key-less face (official Locator::Invalid) stays in its position as an honest hole, and a
  message with no locator keys at all stores no row (no `[{}]` noise). Key material rides the
  internal column only; the host wire descriptor keeps its exact pre-1.56 shape (byte-identical
  `attachments_json` — width/height/caption/blurHash ride it only when a future batch widens
  the wire face, which 1.56 deliberately does not). Ingest caps mirror the §4.13 discipline:
  base64 fields ≤512 chars, hex ≤128 chars, cdnNumber within u32, uploadTimestamp > 0, unknown
  keys ignored, wrong-shaped keys dropped individually (never fatal, never guessed). The
  descriptors themselves stay the §6.7 metadata-only faces this batch: nothing downloads,
  opening one still answers the normal not-found failure; consuming the faces (transit CDN
  download → plaintextHash-or-digest verify → attachment storage, on-demand per click/preview,
  the official `queueAttachmentDownloads`/`AttachmentDownloadManager` semantics —
  `ts/services/backups/import.preload.ts:725-767`) is registered as the attachv2 step-3
  follow-up batch. Schema description updated with the schema-18 column.
- Step-3 pre-ruling, integrity verification (recorded now so the follow-up batch executes
  rather than re-decides; the research doc's open question #2): transit downloads verify in
  two layers when both faces ride the row — ciphertext integrity first, streaming
  HMAC-SHA256 over the downloaded body compared against `digest` (the sender-original
  encryptedDigest; the transit protocol's terminal HMAC — official download path
  `AttachmentCipherInputStream` semantics), then plaintext SHA-256 compared against
  `plaintextHash` after decryption (official `Bytes.toHex` pairing,
  `filePointers.preload.ts:147-158` keeps both forms). A row carrying only plaintextHash
  (the downloaded-once form, transit pointers possibly absent) is the media-tier face and is
  not transit-downloadable by this ruling — the four-piece transit gate
  (`isDownloadableFromTransitTier`, `ts/util/Attachment.std.ts:806-825`) requires digest OR
  plaintextHash plus key+cdnKey+cdnNumber; when only the hash rides, the download may still
  proceed but a hash mismatch is terminal (no digest re-check possible). Failure degrades
  honestly: verification failure or transit 404 keeps the message and answers the attachment
  placeholder (the official tap-to-retry face); nothing auto-retries.
- Step-3 wired (contract 1.57, 2026-10-06): `history.attachmentDownload`
  (send lane, account-scoped mutating, capability `history-import` — no new
  bit, the 1.53 precedent) walks one pointer face end to end. Official
  pipeline anchors, verified against the local Signal-Desktop checkout and
  its GitHub main (AGPL, read-only reference, zero code copied):
  `ts/util/Attachment.std.ts` `isDownloadableFromTransitTier` (integrity +
  key + cdnKey + cdnNumber), `ts/textsecure/WebAPI.preload.ts`
  (`getAttachment` cdnPath `/attachments/<encodeURIComponent(cdnKey)>`,
  `getCheckedCdnUrl` cdn map with the unknown-number `?? cdnUrlObject['0']`
  fallback; production bases `cdn.signal.org`/`cdn2`/`cdn3` per
  `config/production.json`), `ts/AttachmentCrypto.node.ts`
  `decryptAttachmentV2ToSink` (raw body → digest hash → trailing-MAC split →
  AES-256-CBC with the 16-byte IV prefixed → `trimPadding(size)` →
  plaintextHash), `ts/util/appendMacStream.node.ts` (32-byte HMAC key and
  MAC), `ts/util/decipherWithAesKey.node.ts`, `ts/util/trimPadding.node.ts`.
  **Digest-face semantic refinement of the pre-ruling above**: the official
  `digest` is the SHA-256 of the complete body INCLUDING the trailing MAC
  (the pipeline feeds the raw stream into the digest hash before the MAC is
  split off — `checkIntegrity` type 'encrypted' compares that value), while
  the HMAC-SHA256(macKey, IV‖ciphertext) is the transit transport's terminal
  MAC checked against the body's last 32 bytes. The pre-ruling's "streaming
  HMAC-SHA256 vs digest" wording conflated the two; the implementation
  follows the official primitives — both the terminal-HMAC gate and the
  whole-body SHA-256 digest gate verify streaming at the ciphertext layer,
  then the plaintextHash gate verifies over the trimmed plaintext. The
  pre-ruling's other clauses stand as written: a plaintextHash-only row is
  downloadable (the official gate allows it), its mismatch is terminal,
  and no failure auto-retries. Wire shape: params are the
  `messages.attachments.open` addressing quartet; result `{sizeBytes}` —
  the plaintext byte count `messages.attachments.open` reports afterwards;
  there is deliberately no separate status/handle face (the chain is
  synchronous within the request, bytes stay behind the chunked media
  triple). Error mapping: pointer-face holes (missing/malformed
  key/cdnKey/cdnNumber/integrity, missing or out-of-range descriptor size)
  answer INVALID_REQUEST before any network traffic; download, integrity,
  and decryption failures answer UPSTREAM_ERROR retryable=false (terminal —
  a corrupt or expired transit object does not improve on a re-fetch);
  local filesystem failures keep the retryable UPSTREAM_ERROR face.
  Idempotent dedup: an already-present file answers its measured size
  without touching the CDN (an earlier call, or the engine's own live
  download of the same id). Landing: the plaintext writes to a unique temp
  name and renames into the governor-probed `attachments/` directory under
  the descriptor's stable id — the exact file `messages.attachments.open`
  resolves — so the landed bytes ride the existing media governance (TTL +
  quota) unchanged. Network bounds: connect 10 s, total 120 s, redirects
  refused; the ciphertext streams to disk in 64 KiB chunks (resident memory
  one chunk plus the 32-byte MAC holdback), and the decrypt pass re-reads
  the temp file bounded to the ciphertext region. New crates: `aes` 0.8 +
  `cbc` 0.1 (cipher 0.4 line — `cbc` 0.2 pulls cipher 0.5 and does not
  compose with `aes` 0.8), `reqwest` 0.12 (rustls-tls-webpki-roots +
  stream, default features off — rustls keeps the TLS story pure-Rust and
  matches the repo's no-openssl posture); `sha2`/`hmac` were already in
  the tree. Test seam: `history_cdn_base` (launcher-input-only config,
  never set by the production launcher) aims the chain at a local mock CDN
  — the same seam the official desktop carries as its window config cdn
  values. Contract 1.98 transport parity: the download client reads the
  launcher's `KT_SIGNAL_SOCKS_PROXY` process environment — the doctor's
  tolerant parse (canonical `host:port`, an explicit `socks5(h)://` prefix
  normalized; a malformed value reads as unconfigured, everywhere else too)
  — and rides the GET as `socks5h://` (remote DNS), the same mapping the
  engine child gets for `ALL_PROXY`, so the transit-CDN fetch shares the
  route the engine's own transport takes through walled or DNS-poisoned
  networks; a missing value builds the previous direct client, and the
  connect 10 s / total 120 s / no-redirect posture is unchanged.
- Bounded discipline (ADR 0004): the archive is read as a line stream with a hard per-line cap;
  pass 1 stages recipient/chat identity maps into bounded SQLite temp tables (dropped at the
  end), pass 2 streams messages in fixed-size batches (256 rows per transaction, store lock
  released between batches on the blocking pool). Nothing proportional to the archive is held in
  memory. Imported rows do not bump unread badges and emit no per-message host events (a
  100k-message import would otherwise flood the host lane); the desktop refreshes
  conversations/messages after the status reaches a terminal state. Imported text follows the
  receive size ladder (oversized bodies keep only the truncated preview, marked not-retrievable —
  there is no engine copy to fetch); attachment descriptors are metadata-only mirrors of §4.13
  (their ids are synthetic: opening one answers the normal not-found failure — no bytes were
  ever downloaded, §6.7). Quotes resolve their target row when it is already imported or live
  (click-to-scroll works) and always carry the bounded snapshot. `remoteDeleted` items import as
  `remote-deleted` tombstone rows; pinned items import into the conversation-pins table;
  direction-less items, long-text-only bodies (metadata-only overflow), and messages whose
  author/chat cannot be resolved to a stored peer identity are skip-counted. Imported rows carry
  fresh `stored_at`, so the §7.1.1 age rule treats them like any other local copy; one bounded
  retention pass runs after a completed import so the store converges to the per-conversation
  cap within the same session.
- Orchestration: at most one import runs per group at a time. After `link.finish` succeeds in
  kt-engine mode, a bounded watcher polls for the archive (every few seconds, at most ten
  minutes — the engine's own transfer long-poll plus transfer budget) and starts the import when
  the file appears; once per supervisor start, a sweep imports any account whose archive exists
  but has no completed row (connector restarted between link and import, or a previous attempt
  failed). A failed run is retried on a later start up to three attempts, then stays `failed`
  with its error class until the account is deleted or re-linked (only-first-link semantics,
  ADR 0005 §风险与边界). The connector never deletes or rewrites the engine's archive file; the
  completed row in the connector store is the consumption marker.
- Sticker rows land whole (contract 1.77, 2026-10-08; engine counterpart recorded in
  kt-signal-engine `docs/jsonrpc-compat.md` §5): the archive's per-message `sticker` face —
  unprojected since 1.39's tolerant frame list — now parses with the live-receive 1.35 bounds
  as the single source of truth (`engine::MAX_STICKER_*`: packId ≤64 chars, packKey ≤128
  base64, stickerId within u32, emoji ≤32 and optional; the engine's export gate already keeps
  the official shapes on the wire — packId from exactly 16 bytes, packKey from exactly 32 — so
  oversized identities reaching the import are hostile-line territory only). The sticker
  identity is the row's body: a bodyless row carrying a sticker face imports instead of
  skip-counting, and the `data` byte pointer joins the descriptor list exactly as a live
  envelope's sticker normalization does — metadata-only, its `locator` face (when present)
  lands in the internal `attachment_locators` column under the same attachv2 rules, nothing
  downloads. The face rides only whole: a missing identity field or a missing/non-object
  `data` pointer answers no sticker, and the bodyless row behind the dropped face keeps plain
  skip routing — never a half sticker row (the live `normalized_sticker` discipline). The
  import batch writer now also persists `sticker_json` (previously hardwired NULL in its
  INSERT — a latent write-face gap unobservable while the importer projected no sticker).
  Host wire unchanged: the `sticker` field has ridden the message wire face since 1.35, so
  this batch is wire-shape-zero — no schema JSON change, no capability change, no schema
  store version change.
- Contact cards land whole (contract 1.78, 2026-10-08; engine counterpart recorded in
  kt-signal-engine `docs/jsonrpc-compat.md` §5): the archive's per-message `contact` face —
  unprojected since 1.39's tolerant frame list — now parses into a bounded `NormalizedContact`
  card (name parts with the official empty-stays-absent semantics, ≤8 phone/email entries
  each with a value ≤256 chars and the official HOME/MOBILE/WORK/CUSTOM type vocabulary —
  unknown/absent coercing to HOME, the official `phoneToContactFormType` parse face — plus
  an optional label ≤64 chars and organization ≤128 chars; the archive's `address` face is
  not consumed, a recorded v1 gap). The card rides the row's `contact` wire field through
  the rich channel (the §4.37/§4.40 additive route: `NormalizedRich.contact`, persisted in
  `rich_json`, no store schema bump) and the avatar byte pointer joins the descriptor list
  exactly as a live attachment does — metadata-only, its locator face lands in the internal
  `attachment_locators` column, nothing downloads. The card rides only whole: a non-object
  face, or one with no usable field (no name part, no entries, no organization, no avatar),
  answers no card and the bodyless row behind it keeps plain skip routing — never a
  placeholder contact row; entry-level malformation (an empty value) drops the entry, never
  the row. Additive: pre-1.78 rows deserialize without the key and stay byte-identical.
- Group updates land as the live system notice (contract 1.79, 2026-10-08; engine counterpart
  recorded in kt-signal-engine `docs/jsonrpc-compat.md` §5): the archive's per-message
  `groupUpdate` face — the engine's full official `#fromGroupUpdateMessage` detail vocabulary —
  now lands as the same system row a live group update does: 「群聊信息已更新」 with the
  `is_group_update` rich flag and the first `title` detail's new title mirrored into
  `group_name` under the live `GROUP_NAME_MAX_BYTES` bound, persisted through the existing
  rich channel with no schema, capability, or store-version change. The row identity is the
  live receive form over the updater: an outgoing update is the account, anything else
  resolves the archive author exactly as an incoming author does (an unresolvable updater is
  skip-counted, never invented); the conversation title is deliberately not mirrored from the
  update row — the archive's chat frame carries the current title, and a historical notice
  must not regress it mid-import. Whatever the archive row's direction says, the face lands
  as a system row; when the same archive row also carries the timer face (a batched group
  change that moved the timer — the shape §4.41's direction-less export never covered for
  group chats), it splits into the §4.41 timer row and the group row, the official
  `additionalMessages` semantics. Presence without material (`details` missing or empty)
  is no face: the row behind it keeps plain skip routing. A re-run converges through the
  identity discipline, as every import row does.
- The non-text expansion batch lands (contract 1.81, 2026-10-08; engine counterpart 592b1d3
  recorded in kt-signal-engine `docs/jsonrpc-compat.md` §5 递补批; desktop wire shapes fixed
  by kt-desktop 67abb977+42801981): the seven backup5 row types — frame-level `adhocCall`
  call records and the ChatItem `payment`/`giftBadge`/`viewOnce`/`storyReply`/`poll`/
  `adminDeleted` faces, unprojected since 1.39's tolerant frame list — now consume:
  - **viewOnce** needs no import-only parse: the engine projects the live receive keys —
    the top-level `viewOnce`/`isErased`/`viewOnceInvalid` markers with the single medium on
    the row's standard `attachments` list — so the existing attachment pathway and the
    §4.25 `viewOnce`/`viewOnceInvalid` rich flags carry it verbatim (`view_once_invalid` =
    `isErased || viewOnceInvalid`, the erased/invalid form rendering the same placeholder a
    burned live row does). A `viewOnce` row counts as bodyful even with no medium (the
    burned placeholder row).
  - **storyReply**: the face's `body` rides the receive ladder as the row's body (byte
    ladder, honest truncation flag) and `bodyAttachment` marks the metadata-only long-text
    overflow (no descriptor, the standard long-text posture). The story author resolves
    connector-side (the engine exports no story context): an outgoing reply answers the
    conversation peer's story, an incoming reply the account's own, and only a direct
    conversation carries a story context — a group row lands its body plain with the face
    honestly absent. Both resolved forms are existing row identities (peer key, account
    number); nothing is invented. `rich.story_reply.sent_timestamp` pins 0 (stories are
    never imported, so the answered story has no upstream timestamp); the emoji arm (the
    official storyReaction face) has no row face connector-side and honest-skips (a recorded
    v1 gap).
  - **poll**: the engine projects the official import face (options carrying their votes
    nested as `{voterId, voteCount}`); the connector aggregates onto the desktop's read-only
    face — one entry per voter in first-seen order with the option indexes they picked
    (`voterId` + `optionIndexes`), a voter the §4.38 recipient ladder cannot resolve
    dropping vote-level (the official unknown-voterId warn-and-skip, never the row).
    `terminated` is the boolean face of `hasEnded` — the official `terminatedAt` stamp is
    the poll message's own timestamp (the row's `sentAt`), not repeated. The official
    `voteCount` tally has no desktop timeline face and is not transported (recorded
    projection gap). Question/options always ride (the official `?? ''`/`[]` defaults);
    hostile-line caps: options ≤32, votes ≤64, indexes ≤32, question/option text 256 chars —
    truncation drops trailing entries whole, their votes dangling with them, never fatal.
  - **payment** crosses note-only: the engine's face carries `{amountMob?, feeMob?, note?,
    transactionDetailsBase64?}` but amounts, fees and the TransactionDetails blob have no
    desktop render face (KT carries no MobileCoin ledger) — not transported (recorded
    projection gap). The face is structural: an object with no note is still the payment
    notification.
  - **giftBadge** crosses state-only: the engine's capitalized `GiftBadgeStates` vocabulary
    lowercases onto the desktop's wire vocabulary; the receipt-credential presentation (the
    engine's face on every non-FAILED state) stays engine-side — zk credential material with
    no desktop data face (recorded projection gap).
  - **adminDeleted**: the face's presence (the engine always projects `adminId`, 0 included)
    flips the 1.33 `admin_deleted` tombstone flag beside an intact row — the status ladder is
    untouched (the live `mark_admin_deleted` posture), the body (when the archive carried
    one) stays for the audit window. The admin identity has no row face (a recorded v1 gap;
    the official join is a conversation-table name lookup KT cannot make from an opaque id).
  - **adhocCall**: the frame-level call record is recognized (never the junk `skippedLines`
    bucket) and honestly skip-counted — the connector keeps no call-history store and no
    call-link conversation kind (§4.49: the connector is the signaling pipe), so the
    official drop path is total (the official import drops unresolvable call-link rows too).
  All faces ride the additive rich channel (`NormalizedRich.poll`/`payment`/`gift_badge` +
  the existing view-once/story-reply flags), the body gate counts each face as a body, and
  wrong-typed faces (a non-object `payment`, a non-string badge state, a direction-less poll)
  answer no face — the row behind them keeps plain skip routing. No host wire shape beyond
  the additive `poll`/`payment`/`giftBadge` row fields; no schema JSON change, no capability
  change, no store schema version change.

### 4.39 Group member roster: groups.get `members` (contract revision 1.40, 2026-10-05)

The official conversation-info page and the composer @-mention picker both render the group's
member roster. The §4.8 projection carries only `memberCount` — the sync captures the upstream
member list solely to count it (supervisor `sync_contacts_with`, contract 1.9) — so the desktop
has no identity, display-name, self, or admin information for any member. This revision extends
`groups.get` instead of adding a second method: the roster belongs to the same cached row, the
same read-lane, and the same `syncedAt` staleness story; a separate `groups.getMembers` would
duplicate the targeting, the error family, and the cache-read classification for no new
capability.

- Decision — capture at sync, resolve at read: `contacts.sync` keeps the upstream member array
  it already walks, this time inside the group row's `extra` JSON beside `memberCount`
  (`{"memberCount": N, "members": [{"id", "uuid"?, "admin"?}, ...]}` — no store schema-version
  bump; `extra` is the additive channel §4.8 already uses). `groups.get` stays upstream-free
  (§4.8) and projects the array, resolving each member's display name at read time from the same
  account's cached `kind='contact'` rows — one lookup per response, no name copy in the roster
  row, so a name the next sync improves is reflected without a roster rewrite. The result gains
  an optional `members` array; rows synced before this revision carry no `members` key, and the
  response stays byte-identical for them.
- Member shape (the official member row's minimal set): `id` — the member address with the
  connector-wide first-wins convention (`number` before `uuid`, the same ladder the contacts
  sync and the §4.20 mention author use, so roster ids join against contact peer keys, message
  senders, and mention authors without a second identity mapping); `uuid` — present only when
  the upstream supplied it separately (the pinned signal-cli 0.14.8 member record does;
  absent otherwise); `name` — the contacts-cache title, absent when the member is not in the
  cache (a group-only or not-yet-synced peer) — the pinned upstream member records carry no name
  field of their own, so there is no engine raw name to fall back to and none is invented;
  `self` — true when `id` equals the linked account's number (number-based, the §4.24 recorded
  boundary: the own ACI is not queryable upstream, an ACI-keyed roster entry cannot be marked);
  `admin` — present only when the upstream marks the member: pinned signal-cli ships both a
  per-member `isAdmin` flag and a separate group-level `admins` address set (verified in the
  0.14.8 distribution jar, `ListGroupsCommand$JsonGroupMember` / `$JsonGroup`), so the connector
  records `admin: true` when either names the member. `false`/absent collapses to an absent key,
  like `mentionsSelf` (§4.29).
- Engine-mode projection difference (recorded, not worked around): `kt-signal-engine`'s
  `listGroups` returns `members: [{number}]` — one address per member (number first, the ACI
  string where the contacts map has no number), no separate uuid, no admin face. In that mode
  roster entries carry `id` only (plus a read-time `name` when the cache knows the number), and
  `admin`/`uuid` are honestly absent. Neither engine's member record exposes the official
  member label/emoji badges; they are not projected.
- Bounds: the roster cap is the official GroupsV2 group-size ceiling, 1001 members
  (`MAX_GROUP_MEMBERS`); entries past the cap drop, entries with no address drop individually,
  and every address is capped at 128 chars — a hostile upstream payload can never fail the sync
  or the read (the §4.13 discipline). `memberCount` keeps its existing meaning (the upstream
  array length), even in the pathological case where it exceeds the projected roster. The
  roster lives in the row the sync already writes, so its store cost is bounded by the groups
  the account is actually in at upstream roster scale.

### 4.40 Mention author ACI projection (contract revision 1.40, 2026-10-05)

The premise check this revision records first: the mission's "mention ranges are not persisted"
no longer holds — §4.20 (contract 1.25) already persists the normalized mention ranges on the
row (`messages.rich_json`, schema 9) and projects them on the wire row (`mentions:
[{author, name?, start, length}]`, flattened), and both engines deliver them inbound: the pinned
signal-cli 0.14.8 jsonRpc receive carries `JsonDataMessage.mentions` (`JsonMention{name, number,
uuid, start, length}`, verified in the distribution jar), and kt-signal-engine projects the
protocol `body_ranges` MentionAci entries as `{"number": <aci>, start, length}` (≤64, the P2
receive-projection face, engine `3ac097a`). The §4.29 `mentionsSelf` marker and the §4.24 badge
ride those same ranges. What is genuinely missing is the *canonical ACI*: official
Signal-Desktop body ranges key the mentioned identity by ACI (`mentionId`), while the
connector's `author` value is engine-dependent — signal-cli mode resolves it number-first
(an ACI only when the number is unknown), engine mode puts the ACI into the same `author`
string with no way for a host to tell which identity family it is reading.

- `mentions[].authorAci?: string` (wire row field, contract 1.40): present only when the ACI is
  honestly known — the envelope's `uuid` field (signal-cli mode), or the `number`-keyed value
  when it is UUID-shaped (engine mode, where that value *is* the ACI by construction). `author`
  keeps its exact pre-1.40 value and the number-first resolution, so every existing row and
  consumer is byte-identical; `authorAci` is purely additive. A number that merely looks short
  or malformed never promotes — only the strict UUID grammar (`8-4-4-4-12` hex, case-insensitive)
  counts.
- Persistence rides the existing `NormalizedRich` JSON in `messages.rich_json`: new rows carry
  `authorAci` inside their mention entries, pre-1.40 rows deserialize without it and the key
  stays absent on the wire — no store schema-version bump (the §4.37 additive precedent), no
  new cap (the §4.20 per-row 64-mention cap and 128-char id cap already bound the payload),
  no new method or event.
- `mentionsSelf` semantics are deliberately unchanged: the §4.24 verdict stays number-based
  receive-time, never recomputed, because the linked account's own ACI is not queryable on
  either engine's jsonRpc surface (`listAccounts` returns numbers only). An engine-mode
  self-mention (author = the account's ACI) still cannot be attributed to self — the recorded
  under-count boundary stands; `authorAci` does not silently re-decide it.
- Send chain untouched: outbound `messages.sendText` mentions (§4.34 ladder) already pass
  through and this revision adds nothing to them.
- Desktop consumption: an @-chip resolves against the §4.39 roster by `authorAci` when present,
  falling back to `author` (which the roster's `id`/`uuid` pair joins in both engine modes);
  the "@you" highlight keeps reading `mentionsSelf` and needs neither field.

### 4.41 Disappearing messages: receive metadata, timer mirror, local sweeper, conversations.setExpireTimer, history import (contract revision 1.42, 2026-10-05)

The premise this revision implements: Signal's disappearing messages are a protocol feature the
connector must stop swallowing. Upstream, the timer lives in the conversation state and every
message row carries the countdown it was sent with; locally, pre-1.42 receives dropped all of it,
so a linked device silently kept forever what the sender meant to expire. The upstream engine
(E-batch, separate repository) projects the envelope keys and validates the timer set; KT Desktop
(D-batch) renders and triggers; this connector owns the middle: capture, mirror, local expiry of
stored history, the host-facing control method, and the archive import.

- Receive metadata (both engine faces, one parse): a `dataMessage` (direct `dataMessage` or the
  multi-device `syncMessage.sentMessage` mirror) states its conversation's timer with
  `expiresInSeconds` — absent = the envelope states nothing, `0` = the sender's timer is off —
  and, on the engine face only, the countdown start `expirationStartTimestamp` (>0 stamps only;
  a `0` is treated as absent). Both ride the normalized receive verbatim (`expire_seconds`,
  `expire_start_at`); neither is recomputed, defaulted, or clamped: the values are protocol
  state, not connector policy.
- Row persistence (schema 16, additive): `messages.expire_seconds` / `messages.expire_start_at`
  keep the stated keys verbatim; `messages.expire_at` is the connector's sweep deadline, written
  at insert as `start + seconds·1000` (saturating, capped at the `i64` face) where `start` is the
  engine-stated start, else the row's receive time, else the store's own insert stamp — never a
  peer-claimed wall clock. Rows without a live timer (`0`, absent, timer notices) keep all three
  columns NULL, so a conversation without disappearing messages costs zero sweep work. A partial
  index on `expire_at` bounds the sweeper's due-set scan.
- Timer-change mirroring: a bodyless receive flagged `isExpirationUpdate` is a timer update, not
  a message. It lands as a `system` row (status `system`, rich marker `isExpirationUpdate: true`
  as its only rich content) whose text material speaks the announced value —
  `已更新消息定时消失：{N} 秒` for N>0, `已更新消息定时消失：已关闭` for the `0` and absent close
  forms; the desktop localizes presentation from this material. Before the row inserts, the
  conversation's mirror column `conversations.expire_timer_seconds` (NULL unknown / `0` off /
  N seconds) updates, so the `conversation.changed` summary the row's landing already carries
  the new state. A peer change and our own phone-side toggle (which mirrors through
  `sentMessage` addressed by destination) land as the same row shape, so every device's history
  agrees. A timer notice row never sweeps (its record carries no expire columns) — the state is
  in the conversation row; the notice is only the visible event.
- Local sweeper: one bounded pass per group supervisor deletes at most 256 due rows
  (`expire_at <= now`, oldest deadline first) in a single transaction and repairs exactly the
  touched summaries — the same last-message/unread clamp a retention prune applies — plus the
  account aggregates, so a sweep never leaves a summary promising a row the timer ate. A full
  batch drains again immediately (the cap bounds rows, not conversations); otherwise the
  supervisor sleeps the sweep interval (default 30 s, `serve --expire-sweep-interval-ms` /
  `KT_SIGNAL_EXPIRE_SWEEP_INTERVAL_MS`, startup settle 5 s). The pass is single-flight per
  process, and failures leave the rest for the next tick. Every touched conversation emits
  `message.expired` once per batch: `{accountId, conversationId, messageIds}` (non-empty,
  bounded by the batch; groups arrive per conversation with no global ordering guarantee). The
  sweep deletes store rows only — attachment bytes follow the media governor's existing
  retention discipline, identical to a prune — and never touches upstream state.
- `conversations.setExpireTimer` (host method, capability `disappearing-messages`): params
  `{accountId, conversationId, expireSeconds}` with `expireSeconds ≤ 2147483647` (the common
  `i32` denominator of both upstream faces); result `{expireTimerSeconds}`. Only direct
  conversations are settable — a group timer travels through group updates this connector's
  upstream faces do not expose, so a group answers `INVALID_REQUEST` before any upstream call
  (recorded boundary). The upstream payload follows the caller's engine mode verbatim: the
  engine face's `setExpirationTimer {account, recipient, expirationInSeconds}` versus the
  signal-cli face's `updateContact {account, recipient, expiration}` (single-string resolved
  recipient on both). Same-value no-ops are upstream semantics — the passthrough never
  pre-checks. A lost mutating result answers `SEND_OUTCOME_UNKNOWN`, never retried; on a
  definite success the local mirror updates best-effort and the refreshed summary rides
  `conversation.changed`. The upstream timer state stays the source of truth: the mirror only
  speeds up local reads between events.
- Link-time history import (§4.38 pipeline): a chat frame's `expirationTimerMs` (ms → s, rounded
  up so an imported state never under-reports) seeds the conversation mirror; a message frame's
  `expiresInMs` (same ms ladder) and `expireStartDate` (>0) fill the row's expire columns with
  the deadline stamped at import; and the engine's direction-less `expirationTimerChange` frame
  applies the announced state and renders the same timer-notice system row a live change lands
  as — under the live system identity for that conversation, so a re-run, or a live change
  stored first, dedupes through the existing identity tuple. The updater's ACI is deliberately
  not part of the row identity: the notice is conversation-scoped state, the copy speaks the
  timer value, not the actor (recorded boundary — a desktop that wants the actor needs a future
  wire field, not an identity change).
- Not implemented on purpose: per-message expiry timers (Signal's disappear-on-read) and any
  upstream deletion trigger. The connector's sweeper is local storage hygiene with
  host-visible events; the protocol's authoritative expiry stays upstream. (Group timer
  writes: §4.41 originally recorded them as not implemented; §4.44 opened the
  `updateGroup {expiration}` route and the 2026-10-06 dated note there lifted the last
  mode gate.)

### 4.42 Note to Self: the self conversation skeleton + the `isSelf` marker (contract revision 1.43, 2026-10-05)

The premise this revision removes is a connector gate, not an upstream limit. Signal treats the
chat with oneself as a first-class 1:1 conversation: signal-cli ships `RecipientIdentifier.NoteToSelf`
as a first-class recipient and routes a send addressed to the own number/uuid to the account's own
recipient id (`ManagerImpl` send path, pinned 0.14.8), the kt-engine maps the own number to the
Note to Self recipient in its storage sync, and the linked account appears in its own
`listContacts` output. The connector deliberately dropped that entry since the contract-1.14
skeleton sync ("must not become a conversation skeleton"), so the desktop had no entry point and
no data source for Note to Self (desktop plan P1-18). The upstream behavior needs no engine change
in either mode; this revision stops swallowing the entry and marks the resulting conversation on
the wire.

- Contact sync keeps the self entry: the `peer_key == account number` skip in the contacts-sync
  loop is gone, and the linked account's own entry flows through the exact same skeleton path as
  any contact — a cached `kind='contact'` row plus the direct conversation skeleton (§6.5). The
  sync's title composition stays untouched: for the self entry it resolves the account's own
  profile name, and that name is *material*, not presentation — the official client renders the
  conversation title as a localized "Note to Self" and never the own profile name. The desktop
  localizes from `isSelf`; the connector ships no Note to Self copy in any language (the same
  material-vs-copy split the §4.41 timer-notice rows use).
- `isSelf` on every conversation summary (wire key `isSelf`, always serialized like
  `muted`/`pinned`): true exactly when the row is a direct conversation whose `peerKey` equals the
  linked account's number. Computed at read time in the store's summary constructors — zero
  migration, zero storage, and the receive path's existing self-keyed rows (a `sentMessage` mirror
  addressed to the own number) light up without a rewrite. The match is number-based by the
  §4.24 recorded boundary: the own ACI is not queryable on either engine's jsonRpc surface
  (`listAccounts` returns numbers only), so a direct row keyed only by a UUID cannot be marked —
  the contacts-sync address ladder prefers the number, which is the form the self entry carries.
  Groups are never self. History-import rows need no new handling: the §4.38 chat resolver already
  maps the archive's `self` chat onto the direct conversation keyed by the own number, so an
  imported self chat is marked on read like any live one.
- Send-to-self needs no connector change and gets none: `messages.sendText` addressed with the own
  number (by conversation id, or by `peerKey`) passes the ordinary direct-chat path and dispatches
  the upstream `send` verbatim; the pinned upstream routes it to the own recipient (the
  Note-to-Self path), from where it mirrors back to every linked device. There is no recipient
  whitelist in the connector to relax — self was never intercepted on this face.
- Engine-mode contact sync shares the same loop and therefore the same change; the hermetic suite
  exercises it through the engine-mode engine configuration. Whether the real kt-signal-engine
  `listContacts` includes the own entry is an engine-repository fact outside this repo's hermetic
  reach — if a deployed engine face omits it, engine-mode simply has no skeleton until the first
  self message lands (the receive path creates the row either way), and that is a recorded
  boundary, not a connector gap.
- Not implemented on purpose (projected differences, desktop D-batch owns the presentation):
  the syncStorage cloud bits `noteToSelfArchived`/`noteToSelfMarkedUnread` stay unprojected
  (KT archive/unread is local state by §6.7); the official "send state always complete" and
  "voice note always played" self special cases are not special-cased (the optimistic send chain
  is shared with every chat, and voice notes are a prior batch's boundary); no separate
  Note-to-Self entry page or pin exists here or upstream — the entry point is the contact list
  row plus search, exactly like the official client.

### 4.43 Contacts-list `isSelf`: the Note-to-Self marker on the compose contact list (contract revision 1.44, 2026-10-05)

§4.42 (contract 1.43) put the self skeleton on the conversations face but left the entry point
unmarked where the official flow begins: the compose/new-chat contact list. The self entry has
ridden the contacts cache since 1.43 (the sync no longer drops it, §6.5), so `contacts.list`
already returns the row — but with no marker the desktop cannot tell it apart from any other
contact and cannot render the localized "Note to Self" title/avatar the official client shows in
the composer. The marker must agree across the two faces: they are two views of the same entry,
and a desktop that cross-references them (list row → conversation skeleton) would otherwise see
self-ness appear on one face and vanish on the other.

- `isSelf` on every contacts.list row (wire key `isSelf`, always serialized — the §4.42
  convention, no skip-when-false key-absence ambiguity): true exactly when the row is a
  `kind='contact'` entry whose `peerKey` equals the linked account's number — the self entry the
  1.43 sync admission put in the cache. Computed at read time in the store's contact projection
  from the row plus the account record — zero migration, zero storage, and the cached self row
  needs no rewrite; the predicate is the same number match the conversation summaries run, shared
  as one helper so the two faces cannot drift apart.
- Number-based by the §4.24 recorded boundary: the own ACI is not queryable on either engine's
  jsonRpc surface (`listAccounts` returns numbers only), so a UUID-keyed contact row cannot be
  marked — the contacts-sync address ladder prefers the number, which is the form the self entry
  carries, exactly as on the conversations face. Group rows (`kind='group'`) are never self: the
  predicate keeps the kind gate regardless of what a group id collides with.
- The row's `title` stays raw material (the account's own profile name, unchanged since 1.43);
  the desktop localizes the Note-to-Self presentation from `isSelf`, and the connector ships no
  copy in any language (the §4.42 material-vs-copy split).
- Purely additive on a free-form result face (the §4.37/§4.40 precedent): the result row gains
  one always-present key; pre-1.44 hosts that ignore unknown keys read every other field
  byte-identically. No new method, event, error code, or capability key; no cursor change (the
  page key stays `(kind, peer_key)`); no store schema-version bump and no `contacts.sync` change
  — the cache row stores no `isSelf` column, so a resync neither computes nor erases the marker
  (it does not exist at rest). The list assembly stays store-only on the read lane (§6.5): the
  account's own number is read from the account row through the already-held connection guard —
  one lookup per page, the same discipline the §4.42 summary list applies.

### 4.44 Group management writes: groups.update, groups.quit, conversations.setExpireTimer group extension (contract revision 1.45, 2026-10-05)

The official conversation-info page manages the group itself — rename, description, membership,
admin grants, bans, the invite link, permissions, member labels — and the desktop G-batch needs a
write face for it. The premise audit (desktop plan §1.2) verified the pinned signal-cli 0.14.8
JSON-RPC surface reaches nearly all of it through `updateGroup` (`UpdateGroupCommand`, a
`JsonRpcLocalCommand`) and `quitGroup` (`QuitGroupCommand`), and corrected one recorded boundary:
§4.41's "group timer travels through group updates this connector's upstream faces do not
expose" was wrong — the parameter is named `expiration`, not `expire`, and it flows the timer
through `updateGroup` verbatim (`UpdateGroupCommand.java` L127 → L162 `withExpirationTimer`).
This revision opens the write face on the signal-cli JVM/native modes and records the
kt-engine gap honestly; the engine batch (separate repository) owns the method parity later.

- `groups.update` (host method, capability `group-management`, send lane, account-scoped
  mutating): params `{accountId, conversationId, ...changes}` — the group is addressed by
  conversation id like every other conversation face and must resolve to a `kind='group'` row
  before any upstream call (a direct conversation answers `INVALID_REQUEST`); `operationId?` is
  validated by shape and never persisted (the §4.5 convention). The change set is the upstream
  multi-parameter shape, one optional key per concern: `groupName?`, `groupDescription?`,
  `addMembers?`, `removeMembers?`, `promoteAdmins?`, `demoteAdmins?`, `banMembers?`,
  `unbanMembers?`, `linkState?` (`enabled-with-approval` | `enabled-without-approval` |
  `disabled`), `resetLink?` (boolean), `permissionAddMember?` / `permissionEditDetails?` /
  `permissionSendMessages?` (`every-member` | `only-admins`), `memberLabel?`,
  `memberLabelEmoji?`. At least one change parameter must be present — an empty change set
  answers `INVALID_REQUEST` (`resetLink: false` counts as absent: upstream reads the primitive
  as "no reset"). The upstream payload follows the pinned jsonRpc parameter names verbatim
  (verified against `UpdateGroupCommand` dests and the `JsonRpcNamespace` camelCase fallback,
  plus the documented 0.14.8 example): `groupId`, `name`, `description`, `members`,
  `removeMembers`, `admins`, `removeAdmins`, `bans`, `unbans`, `resetLink`, `link`,
  `setPermissionAddMember`, `setPermissionEditDetails`, `setPermissionSendMessages`,
  `memberLabel`, `memberLabelEmoji`. The connector's `linkState` value
  `enabled-without-approval` maps to the upstream choice `enabled` (the upstream spellings are
  `enabled` / `enabled-with-approval` / `disabled`); permission values pass through verbatim.
  The group avatar was deliberately absent at 1.45 — upstream takes a local file path, the
  connector has no host-file upload channel (the media face is download-only, §6.7), and
  inventing a temporary file boundary was out of budget; contract 1.89 reverses that ruling
  with the tri-state `avatar` parameter (the four 1.89 bullets below).
- Bounds and empty-set semantics: member arrays carry at most 1001 entries (the official
  GroupsV2 ceiling, `MAX_GROUP_MEMBERS`), each entry 1–128 chars (the roster address cap);
  `groupName`/`memberLabel` are 1–256 bytes, `groupDescription` up to 4096 bytes,
  `memberLabelEmoji` follows the reaction emoji rule (one grapheme cluster, ≤32 bytes). An empty
  array is `INVALID_REQUEST`: upstream reads absence as "no change" (`UpdateGroup` builder
  fields are null-when-absent) and an explicit empty set would commit a no-delta group change —
  a revision bump broadcast to every member for nothing — so the connector refuses it and the
  host omits the key instead. Payload shape is otherwise the host's: member addresses pass
  through verbatim and the upstream resolves/rejects them (the `contacts.setLocalAlias`
  recipient discipline).
- Contract 1.89 — the `avatar` tri-state parameter (the §4.45 `accounts.setProfile` shape
  verbatim, reversing the 1.45 deliberate absence): params `avatar?` with three states —
  absent = keep the current group avatar, explicit `null` = remove, a
  `data:image/*;base64,` data URI = replace. The Remove and Replace forms count as the change
  set's avatar member (a lone avatar call is a real change, not an empty set), and the value
  rides the upstream payload verbatim: an absent avatar adds no key, an explicit null
  forwards `avatar: null`, a data URI forwards the validated string unchanged — the §4.44
  omission discipline. The three states are carried by a dedicated deserialization type
  (`AvatarUpdate` since 1.90, shared with the repaired setProfile face): serde's
  `Option<Value>` — the setProfile face's 1.46 shape — folds an
  explicit JSON null into "absent" at the option layer, which leaves that face's
  explicit-null arm unreachable on the real wire path (the 1.46 shape
  stayed as-is until 1.90; this face has kept the null reachable end to end). The connector never decodes
  image bytes beyond the budget check and never persists them; the byte face is the engine's
  to interpret.
- Contract 1.89 — the avatar validation ladder: the tri-state rides the deterministic
  pre-upstream ladder in both modes (`data:image/` prefix + `;base64,` marker + non-empty
  subtype, canonical padded base64, decoded 1..=10 MiB — `MAX_AVATAR_BYTES`, the engine's
  stated avatar budget), answering `INVALID_REQUEST` ahead of both mode gates so hosts get
  deterministic param errors first (the §4.44 mode-gate-behind-shape discipline).
- Contract 1.89 — the honest degradation: no bound engine serves the group-avatar byte face
  today. The JVM/native face takes a group avatar as a local file path; the connector has no
  host-file upload channel (§6.7), writing the data URI to a temporary file is out of bounds
  (zero local writes), and forwarding the URI would only become an upstream protocol error —
  so an avatar-carrying call answers `CAPABILITY_UNAVAILABLE` (`retryable=false`) after the
  full local ladder, the §4.68 snapshot-import downgrade shape. The kt-engine face keeps its
  `updateGroup`-debt answer verbatim (both the debt and the gate removal belong to the
  parallel engine batch — the payload side above needs no further connector change when the
  byte face lands).
- Contract 1.89 — the error face: `GROUP_AVATAR_UPLOAD_FAILED` joins the structured-error
  allowlist verbatim (the schema error enum included — the consistency gate proves both
  sides total) as the group-faced sibling of the §4.49 `PROFILE_UPLOAD_FAILED` channel: when
  the engine accepts the byte face but the avatar upload chain fails after the group change
  landed, its partial-success data object `{retryable, textSaved}` rides the host frame
  verbatim with `retryable=true` (the §4.49 `with_data` re-read). A data-less older-engine
  face maps to the fixed retryable=true content-free form. The `STORAGE_*` family semantics
  are unchanged. No new capability (the face stays under `group-management`), no new method,
  event, or store schema.
- Engine modes: JVM/native dispatch `updateGroup` directly. The kt-engine face has no
  `updateGroup`/`quitGroup` outbound method (engine methods.rs audit, desktop plan §1.2), so in
  engine mode both methods answer `CAPABILITY_UNAVAILABLE` (`retryable=false`) after local
  param/conversation validation — the media-ingest engine-downgrade precedent
  (`effective_media_ingest`): an advertised face the selected engine cannot serve fails closed
  with a structured error instead of an upstream protocol error. Local shape errors stay
  `INVALID_REQUEST` in both modes; the mode gate sits behind them so hosts get deterministic
  param errors first. The engine batch removes the gate without a connector contract change.
- Result semantics (the §4.41 mutating discipline, not the pin family's): a definite success
  answers `{"status": "sent"}`; an indeterminate upstream outcome (engine exit or timeout with
  the call in flight) answers the `SEND_OUTCOME_UNKNOWN` error, never retried by the connector.
  No local row is written by the send path itself: group state converges through the receive
  projection below, so the desktop's optimistic update reconciles against the change envelope
  like any other member's edit.
- Group-change receive projection (the contract-1.42 system-row discipline): a bodyless
  `dataMessage` whose `groupInfo.type` is `UPDATE` was dropped as `"skip"` before this revision —
  group changes were invisible locally. They now land as a `system` row whose rich record
  carries `isGroupUpdate: true` as its marker and whose text material speaks the envelope's
  honest content — `群聊信息已更新` (the pinned `JsonGroupInfo` carries `{groupId, groupName,
  revision, type}` only: no actor name, no old/new diff, so no finer copy is invented; the
  desktop localizes presentation from the marker, the §4.41 material-vs-copy split). When the
  envelope states a `groupName`, the conversation's title mirrors it best-effort before the row
  inserts, so the `conversation.changed` the landing already pushes carries the new name without
  a poll (the §4.41 timer-mirror slot); the `groups.get` cache row stays exactly as synced until
  the next sync — `syncedAt` remains the staleness signal, recorded boundary. A timer-notice
  envelope (§4.41) still wins when both flags ride one envelope; the generic projection only
  takes what the timer path leaves.
- `groups.quit` (same lane, capability, and result discipline): params
  `{accountId, conversationId, operationId?}`, group conversations only, payload
  `{"account", "groupId"}` — the pinned upstream `quitGroup` dests. The upstream `--delete`
  local-data flag and the `--admin` last-admin handoff list are deliberately not exposed: the
  official client keeps the conversation as a non-member after leaving (the G-batch renders
  that state), and a last-admin quit without a handoff answers the upstream
  `UserErrorException` verbatim (`UPSTREAM_ERROR` family) — recorded boundary, not a connector
  error code.
- `conversations.setExpireTimer` group extension (the §1.2 翻案, correcting §4.41's recorded
  boundary): a group conversation no longer answers `INVALID_REQUEST` on the JVM/native modes —
  the route becomes the `updateGroup {"account", "groupId", "expiration"}` write (the
  upstream group-update face, int seconds, the same `CallClass::Mutating` discipline,
  `SEND_OUTCOME_UNKNOWN` never retried, mirror + `conversation.changed` unchanged). In
  kt-engine mode the group form keeps a structured `INVALID_REQUEST` — the engine face's
  `setExpirationTimer` is recipient-shaped only — with the reason updated from "not settable
  through this connector" to the engine-batch debt ("engine face pending"); the desktop batch
  owns the matrix wording fix. Direct conversations are untouched in both modes.

> **Dated note (2026-10-06, group-timer consume batch):** the kt-engine gate above is lifted.
> The engine batch landed `updateGroup {account, groupId, expiration}` on the engine face
> (kt-signal-engine `main` @ `216a4c7`, `docs/jsonrpc-compat.md` §3 registration row + §5
> "群 expiration 批"; `{status: "sent"}` success, 0 = off, `GROUP_UPDATE_FAILED` with
> `data.status` = the HTTP status passed through honestly — 403 non-admin / 409 revision
> race / 401 credentials / 400 shape — plus `-32603` parameter faces and
> `STORAGE_UNAVAILABLE`), so all three modes share the one group payload:
> `prepare_conversations_set_expire_timer` collapses its group arm to the same
> `updateGroup` tuple in Jvm/Native/KtEngine, and the former "engine face pending"
> INVALID_REQUEST no longer exists in any mode. No new host method, no schema shape change:
> the method enum, params, and result faces of §4.44 already declare this form, so the
> contract revision stays 1.48 (no version bump; 1.49 remains reserved for the serverGuid
> batch).
>
> The engine batch also opened the receive side: a decrypted group change carrying the
> expiration-timer action is envelope-replaced into `groupUpdated {groupId, revision,
> expireTimerSeconds, editor?}` (groupId = the group master key, standard base64 — the
> `listGroups` id form; expireTimerSeconds = 0 means off; epoch > 7 changes are skipped
> engine-side). The connector consumes it on the receive path (normalized as a control
> receive, the §4.37 view-once precedent): the conversation mirror updates through the
> §4.41 setter — `conversations.expire_timer_seconds` = the announced value, 0 stored
> literally as off, never NULL — and the refreshed summary rides `conversation.changed`,
> so a member's timer edit converges exactly like this group's own edit from the send
> path. The notification is state convergence, not chat content: **no message row and no
> system row lands** (the direct-face timer-notice row is envelope-level semantics; the
> desktop's presentation choice for group timer changes is deferred to the desktop batch
> — recorded here as the open presentation-policy boundary), and a `groupUpdated` naming
> a group nothing local resolves is warn-only dropped (宁缺勿错, no row invented).
> `revision`/`editor` are not persisted: the mirror stores the timer value only, the same
> material the §4.41 direct notice carries (recorded boundary — the desktop can join the
> actor from a future wire field if the presentation batch needs one). The hermetic
> kt-engine end-to-end (`kt_engine_mode_serves_group_expire_timer_and_group_updated`)
> exercises the full ladder: no INVALID_REQUEST → SEND_LOG verbatim → projection inject →
> conversation.changed with the new timer.
>
> 2026-10-09 supersession (contract 1.92, §4.75): the "no message row and no system row
> lands" posture above and the open presentation-policy boundary it recorded are overturned —
> the row half landed (the §4.41 timer-notice row, attributed through the very wire field
> this note's last sentence anticipated: `editor` drives the row's sender face, resolved
> through the recorded ACI→peer-key mapping at landing time). The mirror behavior this note
> describes is unchanged and predates the row.

### 4.45 Account settings writes + blocked read: accounts.updateConfiguration, accounts.setProfile, accounts.setExpireTimer, conversations.getBlocked (contract revision 1.46, 2026-10-05)

The account-level settings face of contract 1.46: three host methods that write the official
account-record settings (read receipts, typing indicators, phone-number sharing mode, unlisted
number, profile, the universal disappearing-message default) and one read method for the
blocked-conversations list. Source of truth for all four is the Signal Storage Service record
family (`AccountRecord` for configuration/profile/universal-timer, the per-conversation
`blocked` bits for the blocked list) — the engine owns every storage-service read-modify-write,
the connector forwards, bounds-checks, and resolves the account context. All four are
engine-extension methods (the pinned signal-cli JSON-RPC has none of them), provided by
`kt-signal-engine` per its `docs/jsonrpc-compat.md` §3/§5 registration block; against the stock
signal-cli face they answer the upstream's method-not-found as `UPSTREAM_ERROR` — the same
declared swap boundary the §4.32–§4.34 engine-extension family records.

- `accounts.updateConfiguration`: params `accountId`, plus at least one of the optional
  `readReceipts` (boolean), `typingIndicators` (boolean), `phoneNumberSharingMode` (enum
  `"everybody"` | `"nobody"` — the official `AccountRecord` PhoneNumberSharingMode values;
  the old `CONTACTS_ONLY` value has been removed from the official proto and is not accepted),
  `unlistedPhoneNumber` (boolean). Only the provided keys are forwarded upstream — the upstream
  writes exactly the provided fields, reads the rest back untouched (official
  updateConfiguration partial-write semantics), and an all-fields-satisfied call is an idempotent
  no-write success. A call carrying no writable key answers `INVALID_REQUEST` before any
  upstream call (the engine would fail-closed `-32603`; the connector refuses it locally
  instead). Result `{"status": "saved"}` on a definite success. Send lane, account-scoped
  mutating.
- `accounts.setProfile`: params `accountId`, `givenName` (required, 1..128 bytes — the official
  desktop ProfileEditor `maxBytes: 128` for both name components, v8.31 ProfileEditor.dom.tsx),
  optional `familyName` (≤ 128 bytes, same source), `about` (≤ 512
  bytes — the official bio `maxByteCount: 512`), `emoji` (≤ 32 bytes, one emoji — the repo-wide
  emoji label bound), and `avatar` with three states: absent = keep the current avatar, data URI
  (`data:image/*;base64,` + canonical base64, decoded ≤ 10 MiB — the engine's stated avatar
  budget, AvatarHelper-scale) = replace, explicit `null` = remove (the remove arm rides the
  dedicated tri-state deserializer shared with §4.44 since 1.90 — the 1.46 `Option<Value>`
  shape folded JSON null into absence, leaving the arm unreachable on the real wire path).
  The text face is a full
  write exactly like the official updateProfile (every upstream write carries the complete
  encrypted profile): a key the caller omits is cleared upstream, so the desktop always sends
  the full text face it wants. Result `{"status": "saved"}`. Send lane, account-scoped
  mutating.
- `accounts.setExpireTimer`: params `accountId`, `expireSeconds` (0..=u32::MAX — the engine
  face validates within u32 and `AccountRecord.universalExpireTimer` is a uint32 second count;
  unlike the per-conversation face there is no signal-cli int counterpart to share a smaller
  denominator with). 0 turns the account-wide disappearing-message default off; a same-value
  set is an idempotent no-write upstream. This is the account-level default timer — the
  official "universal timer" (`AccountRecord.universalExpireTimer`): per-conversation timers
  stay on `conversations.setExpireTimer` (§4.41/§4.44), and unlike that face the universal
  timer carries no expiration-update broadcast (other devices converge through storage sync).
  Result `{"status": "saved"}`. Send lane, account-scoped mutating.
- `conversations.getBlocked`: params `accountId`. Result `{blocked: [{conversationId, kind}]}`
  with the same `contact` | `group` | `legacyGroup` enum and entry shape as the §4.33 pinned
  face, in storage order, capped at 256 entries (KT bound — the official clients apply no
  explicit cap; the connector truncates beyond 256 to keep one host frame bounded; entries
  missing identity or kind drop, mirroring the pinned projection). Read lane, non-mutating.
  Blocked state lives in the per-conversation storage records' `blocked` bit, not in the
  account record — a read face with no write counterpart in this batch (unblocking rides the
  engine's storage write face when a desktop needs it; the v1 result carries no
  `blockedAtTimestamp`, matching the engine's §5 registration).

Local validation is the deterministic ladder before any upstream call: `accountId` through the
opaque-id validator and resolved to the owning proxy group (the upstream account key is the
resolved `signal_account`, never the host's opaque id); profile string bounds as above;
`phoneNumberSharingMode` against the two-value enum; `avatar` data-URI shape (image family,
canonical base64, ≤ 10 MiB decoded) — everything else fails at params deserialization
(`deny_unknown_fields`, required keys, boolean/u32 types). Engine errors pass through
structurally: the `STORAGE_*` family (`STORAGE_UNAVAILABLE` / `STORAGE_READ_FAILED` /
`STORAGE_WRITE_FAILED` — avatar upload failures fold into `STORAGE_WRITE_FAILED` upstream,
no new code) maps verbatim with `retryable=true`, and the allowlist already declares these
codes (§4.33/§4.34); an engine `-32603` for a parameter shape this connector's ladder already
refused is unreachable, and any other engine error degrades to the plain `UPSTREAM_ERROR`.
Engine-down and method-absent follow the §4.32 mapping (`RUNTIME_NOT_RUNNING` /
`CAPABILITY_UNAVAILABLE`). Handshake `capabilities` gains `account-settings` (the three write
faces) and `blocked-conversations` (the read face).

Persistence and cross-face boundaries: none of the four methods reads or writes connector
state — no row, no cache, no event, no store schema change; the cloud record is the whole
state and the desktop pulls with the read faces (poll-on-open; there is no storage-sync
push subscription on this batch, mirroring the §4.33 pin face). The AccountRecord 52–60
fidelity fix the engine landed alongside this batch (vendored official proto) needs no
connector change: the connector's storage-write faces (`conversations.setPinned`,
`stickerPacks.setSync`, and this batch) forward only semantic parameters — no record bytes
ever cross the connector boundary — so there is no passthrough surface the fix could regress;
the existing pin/sync integration tests staying green is the regression check.

### 4.46 Android local backup export: backups.export, backups.exportStatus, backups.exportFile.open/readChunk/closeHandle (contract revision 1.47, 2026-10-05) — implemented 2026-10-06 (connector implementation batch)

The connector side of the engine's Android local-backup export (kt-signal-engine
`docs/backup-export-contract.md` §10 — the consumption wiring both sides document): the
connector exports its message store into a one-JSON-object-per-line artifact in the engine's
per-account data directory, starts the engine's background export task over that artifact,
polls the task to a terminal state, and streams the produced snapshot's two files (metadata,
main) back through a media-three-chain-shaped file face for delivery to an Android device.
The 2026-10-05 documentation batch registered the five params defs, lanes, capability tag,
bounds, skip-count semantics, and error mapping; the 2026-10-06 implementation batch landed
the wire registration (request-frame method enum, host dispatch literals,
`FEATURE_CAPABILITIES` `backup-export` tag, the five engine structured-error codes in the
`ENGINE_STRUCTURED_ERROR_CODES` allowlist plus the connector-local `BACKUP_SNAPSHOT_MISSING`
`ApiError` literal, schema method-enum entries, allOf param branches, and error-enum entries)
and the code: `src/backup_export.rs` (artifact builder + snapshot caretaker + backup file
handle table), the supervisor faces, the registry routing, and the tests. The schema-consistency
gates pass both ways: method enum ↔ dispatch literals, error enum ↔ `ApiError` literals.

- IPC shape (engine-side ruling, backup-export-contract §10.1): file-artifact handoff + engine
  background task + connector polling. The engine's backup module is store-free by design, the
  engine's 1 MiB JSON-RPC line cap rules out inline bulk RPC, and the artifact channel has the
  `history-import.ndjson` precedent (§4.38) in the opposite direction. The artifact is
  transitional: the engine deletes it when the export task leaves `running` (success or any
  failure), because the engine must not persist chat content (its AGENTS.md boundary); the
  connector therefore treats the artifact as write-once, never re-reads it, and rebuilds it
  before each export attempt.
- Artifact contract (engine backup-export-contract §10.2): path
  `<engine data-dir>/<account store dir>/backup-export-input.ndjson` — the same directory the
  engine's `history-import.ndjson` uses, resolved through the engine's `engine-state.json`
  registry (`<dir>` per number, compat §1). Line format: first line exactly one
  `{"type":"account", profileKey, givenName?, familyName?, bioText?, bioEmoji?}`; then
  `contact {id, aci?, pni?, e164?, username?, profileKey?, givenName?, familyName?,
  registration?}` (givenName/familyName map to the Contact proto's profileGivenName/
  profileFamilyName — the Contact proto has no bio fields, bio belongs to the account line
  only), `group {id, masterKey}`, `chat {id, recipient}`, `chat_item
  {chat, author, dateSent, direction, dateReceived, read?, body}`, `all_chat_folder {}`
  (optional — the engine auto-appends it when absent). 16-byte fields are 32-char lowercase
  hex, 32-byte fields 64-char lowercase hex; `e164` is a non-zero number; reference ids are
  connector-chosen `u64 ≥ 1` with `0` reserved for the engine's self recipient; every
  referenced row must precede its referencing row. The engine never drops a line silently:
  any unknown type, order violation, dangling reference, duplicate id, bad hex, or empty/
  oversized body fails the whole task (`BACKUP_ARTIFACT_INVALID`) — skip decisions belong to
  the connector at artifact-build time and are counted per cause (below).
- Artifact caps (identical numbers in the engine contract §10.2, the schema defs, and the
  future implementation constants — the schema-consistency pairs for the implementable ones
  land with the implementation batch): per-line ≤ 1 MiB (1,048,576 bytes); total ≤ 4,000,000
  lines; total ≤ 2 GiB (2,147,483,648 bytes); `chat_item.body` ≤ 128 KiB (131,072 bytes,
  engine state-machine enforced); `exportStatus` snapshot list ≤ 32 entries.
- `backups.export`: params `accountId`. Send lane, account-scoped mutating (joins the
  reaction-class write-lane mutex and delete drain barrier verbatim — the engine-side export
  is account-mutating state). The connector resolves the account, (re)builds the artifact from
  its store, then calls the engine `backup.export` (engine extension method per the engine's
  jsonrpc-compat §3/§5 registration; against stock signal-cli it answers `UPSTREAM_ERROR` per
  the §4.32–§4.34 swap boundary). Result `{"status": "started"}` forwarded from the engine's
  `{"status": "started"}`. Before the engine call the connector runs its deterministic ladder:
  account resolution (`ACCOUNT_NOT_FOUND`), artifact build success (a store read failure
  surfaces `STORAGE_READ_FAILED` — the artifact is a store projection), artifact caps
  self-check (`INVALID_REQUEST` if the connector's own build exceeds a cap — the engine would
  fail-closed anyway).
- `backups.exportStatus`: params `accountId`. Read lane, non-mutating, read-metrics class;
  no new error codes for the method itself (unknown account `ACCOUNT_NOT_FOUND`). Result
  mirrors the engine's `backup.exportStatus` honestly:
  `{state: idle|running|completed|failed, artifactPresent, startedAtMs?, finishedAtMs?,
  lines?, frames?, snapshot?: {name, mainBytes, frames?}, error?, snapshots: [{name,
  mainBytes, frames?}]}`. Desktops poll while `running` — deliberately no push event (§4.38
  precedent: one number ticking, not low-latency delivery). Engine restart resets the task
  registry to `idle`; on-disk snapshots remain and stay listed.
- `backups.exportFile.open` / `readChunk` / `closeHandle`: the media three-chain shape
  (§ messages.attachments.open/readChunk/closeHandle, ADR 0004) reused verbatim — open
  `{accountId, file: "metadata" | "main"}` → `{fileHandle, sizeBytes, chunkBytes}`;
  readChunk `{fileHandle, offset}` strictly sequential → `{dataBase64, sizeBytes, eof}`;
  closeHandle `{fileHandle}` → `{released}` (idempotent). Read lane. **Only the latest
  snapshot's two fixed file names are addressable** — there is no path parameter and the
  connector never exposes arbitrary paths (AGENTS.md boundary): `file` is a two-value enum,
  and "latest snapshot" is the newest `signal-backup-<UTC ts>` directory in the account's
  `SignalBackups/` root as reported by `backups.exportStatus.snapshots`. Bounds mirror the
  media chain: chunkBytes 262144 raw (dataBase64 ≤ 349,528 chars), handle TTL 300 s,
  ≤ 8 live handles per account, early release idempotent, unknown/closed handle
  `INVALID_REQUEST`, snapshot vanished mid-stream `UPSTREAM_ERROR`. No snapshot exists yet →
  `BACKUP_SNAPSHOT_MISSING` (new connector code, below).
- Snapshot lifecycle (engine backup-export-contract §10.6): cleanup ownership is the
  connector's — keep the most recent snapshot, delete older ones (and any `-tmp` residue)
  before starting the next export. The engine never deletes snapshots; it only lists them in
  `exportStatus`. Same-second snapshot collisions fail the engine task with
  `BACKUP_SNAPSHOT_EXISTS`; the connector clears and retries.
- Skip-count semantics (connector-side, at artifact build; the engine has no skip face): every
  store row or column that cannot be projected into the v1 artifact line set is counted per
  cause and reported through `backups.exportStatus` extension counters (implemented as the
  `SkipCounters` block merged into every status answer after the first successful build —
  zeros included, absent before any build): `skippedReactions` from `message_events`,
  `skippedEdits` from `message_edits`,
  `skippedQuotes` from `messages.quote_message_id/quote_snapshot`, `skippedAttachments` from
  `messages.attachments_json`, `skippedViewOnce` from view-once rows, `skippedStickers` from
  `messages.sticker_json`, `skippedSkeletonChats` for contacts-sync skeletons without peer
  identity (§6.5), `skippedBodyTruncated` for `messages.body_truncated = 1` rows — a
  truncated preview must not become the canonical restored body — and `skippedRows` for
  every other cause (system rows, remote/admin-delete tombstones, unconfirmed outgoing
  rows, empty bodies, and items whose author does not resolve to a projected contact).
  Exported rows: direction,
  sender resolution (`sender_id` → contact/group ref, self → 0), `sent_at` → `dateSent`,
  `received_at` → `dateReceived`, `body` (the persisted body, ≤ 128 KiB by the store bound),
  per-conversation `expire_timer_seconds` is a v1 gap (the engine's chat line set has no
  timer fields this batch — registered as an implementation-batch item with the engine's
  chat-timer frame face).
- Capability tag `backup-export` joins `FEATURE_CAPABILITIES` and the handshake capability
  assertion; desktops gate all five methods on it.
- Error mapping plan (engine codes map verbatim into the connector structured whitelist —
  the whitelist edit itself is an implementation-batch code change): `BACKUP_EXPORT_ALREADY_RUNNING`
  (retryable=false — poll `backups.exportStatus` instead of retrying),
  `BACKUP_ARTIFACT_MISSING` (false — rebuild the artifact first),
  `BACKUP_ARTIFACT_INVALID` (false — the engine deleted the artifact; fix the builder),
  `BACKUP_SNAPSHOT_EXISTS` (true — clear and retry), `BACKUP_IO_FAILED` (true), plus the new
  connector-side `BACKUP_SNAPSHOT_MISSING` (false — nothing exported yet) for the file face.
  The engine's parameter-face `-32603` (unregistered/fake account, missing AEP) stays inside
  the plain `UPSTREAM_ERROR` fold like every account-domain method; engine-down /
  method-absent follow the §4.32 mapping (`RUNTIME_NOT_RUNNING` / `CAPABILITY_UNAVAILABLE`).
  Schema error-enum additions for the six codes land with the implementation batch together
  with their `ApiError` literals (the schema-consistency error-code gate hard-diffs the enum
  against code literals).

Implementation batch record (2026-10-06) — deviations and decisions as built:

- **Roster members ride the contact set.** A group's §4.39 roster (`extra.members`, `id` +
  optional `uuid` per member) projects its non-self members into the contact set so group
  chat items resolve their authors; one member's two spellings merge into one identity, and
  the linked account's own membership form is skipped (self is the engine's reserved
  recipient, never a contact row). The plan text above ("sender resolution → contact/group
  ref") is realized exactly this way; the alternative — no roster contacts — would have
  skipped every group item as `skippedRows`.
- **`read` is never emitted on chat items.** The store has no inbound read state (contract
  1.32 read receipts stamp only outgoing rows), so the artifact omits the optional `read`
  field rather than fabricating it; restored Android reads stay unread, which matches what
  this connector can honestly know.
- **Counters publish with zeros.** The `SkipCounters` block merges into every
  `exportStatus` answer after the first successful build (zeros included); before any build
  the block stays absent — honest absence, never fabricated zeros.
- **JVM/native group-id decode boundary.** The group id decode mirrors the engine's
  `decode_group_id` (standard base64 of the 32-byte master key → 64-hex `masterKey`); an
  id that does not decode to 32 bytes is a counted skeleton. On JVM/native signal-cli
  engines (no `backup.export`) the start call answers `UPSTREAM_ERROR` (retryable) under
  the §4.32 swap boundary — the export capability is a kt-engine feature, and this
  connector build targets it; the JVM/native path fails honestly rather than guessing.
- **Delete paths release backup handles.** All three `deleteLocalAccountData` completion
  paths (idempotent replay, reconcile short-circuit, confirmed delete) clear the account's
  live backup file handles beside the media handles (ADR 0004 gate 3 verbatim).
- **Unknown start outcome.** `backup.export` is Mutating: an indeterminate outcome answers
  `SEND_OUTCOME_UNKNOWN` (retryable=false) — the task may already be running upstream; the
  desktop polls `backups.exportStatus` to learn the truth. An engine-down or
  unknown-outcome status poll degrades to the store-side disk-truth projection (`idle` +
  snapshot list) instead of inventing a task state.
- **Author resolution covers every store spelling.** The sender reverse map registers each
  contact's canonical forms plus the raw peer-key/roster strings under both conversation
  namespaces (`stable_hash_id([accountId, kind, source])` is what the receive path stamps),
  so an incoming row resolves whichever spelling its envelope carried; an unresolvable
  author is a counted `skippedRows`, never a dangling reference.
- Tests: `tests/backup_export.rs` (end-to-end over the fixture engine: artifact shape,
  snapshot streaming, sentinel error ladder, group/Note-to-Self projection, per-cause skip
  counters, `BACKUP_SNAPSHOT_MISSING` on the empty account), `src/backup_export.rs` unit
  tests (identity parsing, timestamp shape, snapshot caretaker, handle-table ladder), the
  handshake capability assertion in `tests/server_integration.rs`, and the
  schema-consistency gates (methods, error codes, bounds inventory).

### 4.47 Conversation avatar read + blocked write: conversations.getAvatar, conversations.setBlocked (contract revision 1.48, 2026-10-06)
`docs/jsonrpc-compat.md` §3/§5 registration + §4.36-anchored error faces): the desktop-facing
wire methods for the engine's two alignment capabilities — the conversation avatar download
and the per-conversation blocked-bit write. Both are engine-extension methods (the pinned
signal-cli JSON-RPC has neither), dispatched by the engine per its compat §3 registration;
against the stock signal-cli face they answer the upstream's method-not-found as
`UPSTREAM_ERROR` — the same declared swap boundary the §4.32–§4.34/§4.45 engine-extension
family records. `conversations.setBlocked` reuses the §4.45 `conversations.getBlocked` result
face verbatim (`{blocked: [{conversationId, kind}]}`, storage order, the same 256-entry KT
cap and missing-identity drop); `conversations.getAvatar` projects the engine's result
1:1 — `dataUri` (a `data:<mime>;base64,` URI, MIME sniffed from the magic bytes engine-side)
plus `byteSize`, the decoded byte count. The connector forwards the conversation reference
and projects the result; it never decodes image bytes beyond the result-face bound, never
logs avatar bytes or blocked-list contents, and persists nothing (the desktop pulls; no push
event, mirroring the §4.33/§4.45 read faces).

- `conversations.getAvatar`: params `accountId`, `conversationId` (1..256 chars — the shared
  conversation bound; the engine classifies the shape itself: an ACI/PNI ServiceId UUID goes
  straight through, a phone-number text reverse-resolves through the account's contacts, and
  a 32-byte standard-base64 group master key is the `listGroups` id form; any other shape
  fails closed at the engine as `-32603`, which the connector's existing parameter-face
  fallback folds into `UPSTREAM_ERROR`). The connector resolves the local conversation row
  (`CONVERSATION_NOT_FOUND` before any upstream call) and forwards the row's `peer_key` —
  the resolved upstream reference, never the host's opaque local id (the §4.33 setPinned
  discipline). Result `{dataUri, byteSize}`. Result-face bound: the engine caps a download
  at 10 MiB decoded (`AVATAR_TOO_LARGE` beyond), so the data URI stays under the
  10 MiB + base64-expansion envelope (≤ 13,981,080 chars — the §4.45 upload schema margin,
  reused as the result face's coarse guard with a description noting the code enforces the
  precise decoded budget; a `byteSize` that disagrees with the decoded count is a protocol
  error). Read lane, non-mutating — a download touches no account state; the result rides
  one bounded host frame (≤ 16 MiB line budget, §4.12).
- `conversations.setBlocked`: params `accountId`, `conversationId`, `kind` (the §4.33
  `contact` | `group` | `legacyGroup` enum), `blocked` (required boolean — there is no
  toggle form). Send lane, account-scoped mutating — the reaction-class write-lane mutex and
  delete drain barrier verbatim; an indeterminate outcome answers `SEND_OUTCOME_UNKNOWN` and
  is never retried. The engine writes the official storage record's blocked bit
  (storageRecordOps semantics: first block stamps the engine time, an already-blocked record
  is idempotent and never rewrites another device's timestamp, unblock clears it) and
  returns the post-write blocked list in storage order, which the connector projects through
  the same cap-and-drop discipline as `conversations.getBlocked` (§4.45). The conversation
  resolves through the local ladder like every storage write (`CONVERSATION_NOT_FOUND`
  before any upstream call; the upstream keys are the resolved peer reference and kind,
  never the host's opaque id). Result `{blocked: [...]}` — the post-write cloud state read
  back, not a local echo. Unblocking is the `blocked: false` form of the same method; there
  is no separate unblock face.

Structured engine errors (engine compat §3, `-1` + message literals): the blocked write
rides the existing `STORAGE_*` family and `CONVERSATION_NOT_RESOLVED` exactly as the §4.33
pinned write face — all four already declared in the engine.rs allowlist and the schema
error enum, mapped verbatim (`STORAGE_*` retryable=true; `CONVERSATION_NOT_RESOLVED`
retryable=false, the deterministic refusal). The avatar face adds three engine codes to the
allowlist and the schema error enum: `AVATAR_UNAVAILABLE` (the honest "no avatar": the peer
or group has none, the profile key was never synced, or the server answered 403/404 —
`retryable=false`: the state is the answer, not a failure, and the desktop renders its
placeholder immediately), `AVATAR_FETCH_FAILED` (network/download/decrypt failure,
`retryable=true` — a later attempt may succeed), and `AVATAR_TOO_LARGE` (beyond the engine's
10 MiB cap, `retryable=false` — deterministic: the same avatar does not shrink between
attempts). The engine's parameter-face `-32603` (unknown conversation shape, unregistered
account) folds into the plain `UPSTREAM_ERROR` like every account-domain method; engine-down
/ method-absent follow the §4.32 mapping (`RUNTIME_NOT_RUNNING` / `CAPABILITY_UNAVAILABLE`).

Capability tags: `avatar-fetch` (the getAvatar face) and `blocked-write` (the setBlocked
face) join `FEATURE_CAPABILITIES` and the handshake capability assertion; desktops gate the
two methods on them. `reportSpam` (engine alignment batch ③a's third face) is deliberately
NOT in this batch: it needs connector-side per-message `serverGuid` persistence to address
received rows, which lands with the row-level persistence batch — recorded here as the
§4.47 backlog item.

Local validation ladder: `accountId` through the opaque-id validator and resolved to the
owning proxy group; `conversationId` 1..256 chars and resolved to the conversation row;
`kind` against the three-value enum (setBlocked); `blocked` required boolean (setBlocked,
params-shaped). Engine errors pass through structurally per the mapping above.

Persistence and cross-face boundaries: neither method reads or writes connector state — no
row, no cache, no event, no store schema change; the avatar bytes exist only inside the
engine and the response, the blocked state lives in the account's storage records, and the
connector persists neither. No avatar bytes and no blocked-list contents ever reach a log
line (the AGENTS.md redaction boundary; neither path logs payload content).
### 4.48 Report spam: messages.reportSpam + row-level serverGuid persistence (contract revision 1.49, 2026-10-06)

The connector side of the engine's report face (kt-signal-engine
`docs/jsonrpc-compat.md` §3/§5 `reportSpam` registration, 2026-10-06 real KT extension):
reporting a message to Signal's anti-spam pipeline needs the server-assigned message GUID —
the engine projects it on the receive envelope's public metadata (`serverGuid`, proto
projection only, never invented), and the upstream REST call is
`POST v1/messages/report/{senderAci}/{serverGuid}` per entry. The GUID therefore must be
persisted per message row at receive time, and the report method resolves it from those rows:
the desktop never sees or carries a server GUID. This revision lands both halves — the
persistence (schema v17) and the wire method — as connector-first scope; the desktop
consumption is a separate batch.

- Row-level persistence (schema v17): `messages.server_guid TEXT NULL`, a pure additive
  migration on the `table_has_column` pattern — a pre-1.49 store gains the column on open,
  old rows keep NULL, and the column is never backfilled (a GUID a past envelope carried was
  never stored; inventing one would report the wrong upstream message). Every message SELECT
  column list gains the column (the 1.42 expire-column precedent), and `NormalizedReceive`
  carries `server_guid` (serde-skipped, connector-internal — never on the wire) so the
  ingest path writes the envelope's `serverGuid` verbatim into the row, including the
  multi-device sent mirror and system rows when the envelope carries the key. Link-time
  history import (§4.38) stays live-receive-only: the backup5 archive frames carry no server
  GUID (the engine's backup export contract has no such field — verified against the engine's
  chat_item line set), so imported rows keep NULL and the column rides the batch insert as an
  explicit NULL.
- `messages.reportSpam`: params `{accountId, conversationId, messageIds: string[]}`. Send
  lane, account-scoped mutating (reaction-class write-lane mutex and delete drain barrier) —
  the engine face is a protocol uplink under the account's credentials, not a read. Bounds:
  `messageIds` 1–64 entries (the KT UI batch — one screen's selection; the engine's own
  per-batch ceiling is 1000, far above), each 1–128 characters (the opaqueId family);
  `conversationId` within the 256-char pin-face bound.
- Local validation ladder, all before any upstream call: account resolution
  (`ACCOUNT_NOT_FOUND`), conversation resolution (`CONVERSATION_NOT_FOUND`), non-empty
  `messageIds` within bounds (`INVALID_REQUEST`), then per-id GUID resolution from the
  conversation's rows. **A missing row or a NULL `server_guid` refuses that id into the
  per-id failure list instead of failing the request** — partial persistence is an honest
  partial report, and a whole-batch refusal would let one stale row veto the rest. The
  report target (the peer's ACI/PNI, the engine `targetAci` parameter) resolves from the
  conversation: a direct conversation whose `peer_key` is already a UUID passes it verbatim;
  otherwise the §4.37 `peer_identities` mapping resolves the peer key's ACI (the same ladder
  the view-once open sync uses, in reverse); number-shaped keys with no recorded ACI answer
  `INVALID_REQUEST` (the engine requires a ServiceId and the connector cannot invent one).
  Group conversations answer `INVALID_REQUEST` before any upstream call: the engine face
  reports individual senders, and v1 exposes no per-sender selection inside a group.
- Upstream shape: the connector calls the engine `reportSpam
  {account, targetAci, serverGuids: [...normalized], token?}` (engine parameter names per the
  engine compat doc; `token` is not exposed this revision — the envelope-level report token
  is dropped by the engine's own projection, so callers have none to pass). The engine
  answers `{reported, failures: [{serverGuid, status?}]}` (status = the server's HTTP status
  code, absent on transport failures) and the connector projects that result verbatim onto
  its own response `{reported, failures: [{messageId, code?}]}` — the per-id failure key is
  the caller's `messageId`, the value is the engine's status code when the server answered
  and absent otherwise; zero message content ever crosses. When the whole batch fails the
  engine answers the structured error `REPORT_SPAM_FAILED` (`-1` + message), which joins the
  connector structured whitelist verbatim (retryable=false — the server rejected the
  report; the desktop should not blindly re-report). The engine's parameter-face `-32603`
  stays inside the plain `UPSTREAM_ERROR` fold like every account-domain method; engine-down
  / method-absent follow the §4.32 mapping (`RUNTIME_NOT_RUNNING` / `CAPABILITY_UNAVAILABLE`).
  Zero local writes: the report action itself never touches a row.
- Dispatch log discipline: the engine logs the dispatch on its side (protocol uplink, the
  send family); the connector adds no local log line and no event.
- Capability tag `report-spam` joins `FEATURE_CAPABILITIES` and the handshake capability
  list — desktops gate the method on it so an old connector answers a clean capability gap
  instead of `METHOD_NOT_ALLOWED`. Schema: method enum + request branch +
  `messagesReportSpamParams` (messageIds array bounds mirrored in the schema) this batch;
  the matrix reference is left for the desktop consumption batch.

### 4.49 Structured-error data pass-through: PROFILE_UPLOAD_FAILED (contract revision 1.50, 2026-10-06)

The connector side of the engine profile batch ② (8eabf67) error face: `setProfile`'s avatar
upload chain can fail after the text face landed, and the engine answers
`PROFILE_UPLOAD_FAILED` (`-1` + message literal) with a data object
`{retryable: true, textSaved}` — the honest partial-success report. Before this revision the
connector's `ENGINE_STRUCTURED_ERROR_CODES` allowlist did not carry the literal (the error
degraded to plain `UPSTREAM_ERROR` and the engine's `error.data` was dropped at the
`EngineError` boundary), which made the desktop's partial-success dialog dead code against a
real engine. This revision lands both halves:

- Allowlist: `PROFILE_UPLOAD_FAILED` joins `ENGINE_STRUCTURED_ERROR_CODES` (and the schema
  error enum — the schema-consistency gate now proves both sides stay total).
- Data pass-through: the engine's `error.data` object (when present and an object) rides the
  `EngineError::UpstreamCode` variant verbatim into `into_api`, which maps
  `PROFILE_UPLOAD_FAILED` through `ApiError::with_data` — retryable re-read from
  `data.retryable` (falling back to false when absent), the payload serialized on the host
  error frame's optional `data` member. Every other allowlisted code keeps its fixed
  content-free face; their engine-side data (if any) is dropped here rather than leaking
  engine internals unpromised. A `PROFILE_UPLOAD_FAILED` without a data object (an older
  engine face) maps to the fixed retryable=true face with no `data` member.
- Shape discipline: the connector never invents a `data` object and never strips keys from
  one the engine sent; hosts predating this revision ignore the unknown member. No other
  method, event, capability, or schema face changes in 1.50 — the revision records the error
  frame widening only.
- Tests: the schema-consistency error-code gate covers the new literal; the allowlist
  totality test (`ENGINE_STRUCTURED_ERROR_CODES_FOR_TESTS`) proves the service mapping stays
  total; with-data / without-data / non-object-data classification tests pin the boundary.

### 4.49 Call signaling: calls.sendSignal, calls.getState, the call.incoming event (contract revision 1.51, 2026-10-06)

The P1 signaling-passthrough batch (kt-signal-engine `main` @ `8eabf67`,
`docs/jsonrpc-compat.md` §3 `sendCallMessage` + the §5 通话消息块 receive projections,
2026-10-06 real). Official anchor: Signal-Desktop's 1:1 calling rides RingRTC whose wire
face is the Content.callMessage=3 CallMessage (offer/answer/hangup/busy/iceCandidates
frames keyed by a callId, the modern face an opaque Signal-RTC signaling blob with a
droppable/handleImmediately urgency) — this batch is **behavior alignment, AGPL zero
copy**: the connector transparently forwards the signaling material and implements no
calling logic of its own.

Projection boundary (recorded honestly): the connector is the signaling **pipe** between
the engine's transport and the desktop + calling sidecar's RingRTC state machine. The
connector does not run a RingRTC state machine, does not parse or negotiate SDP, does not
configure TURN/STUN, and keeps no call state — those live in the desktop + calling
sidecar (P2 batch, separate repo) and in the engine's transport. Group calls (SFU
signaling) are out of scope through P3 and land no earlier than P4; the upstream
`sendCallMessage` face is contact-only fail-closed and the connector refuses group
conversations locally on the same shape.

- `calls.sendSignal`: params `accountId`, `conversationId` (the shared 1..256 bound),
  `type` (the five-value frame-family enum `offer|answer|ice|busy|hangup`), `callId`
  (positive u32 — the legacy frames' id face and the state machines' shared address
  space; 0 is the no-call marker and is rejected), `payload` (non-empty string ≤
  3,145,728 chars — the same coarse budget the receive face enforces). The shape gate is
  the connector's entire validation: the signaling content itself is never parsed,
  classified, or logged (the engine owns the transport face, RingRTC parses the frames).
  The conversation resolves through the local ladder (`CONVERSATION_NOT_FOUND` before
  any upstream call) and must be `direct` — a group conversation answers
  `INVALID_REQUEST` locally (the upstream fail-closed, mirrored). The upstream call is
  the engine's `sendCallMessage` with `{account: resolved signal number, recipient:
  peer reference, data: payload}` — the resolved upstream reference, never the host's
  opaque local id (the §4.33 discipline). Send lane, account-scoped mutating — the
  reaction-class write-lane mutex and delete drain barrier verbatim; an indeterminate
  outcome answers `SEND_OUTCOME_UNKNOWN` and is never retried. Result `{sentAt}` — the
  engine's envelope timestamp as the send ack, not call state. The `type` value gates
  but does not travel: the opaque payload is self-describing on the modern face.
- `calls.getState`: params `accountId`. Read lane, repeatable, store-only — no engine
  call, no network, no mutation (the `history.importStatus` class). The result is the
  honest empty registry: `{calls: [], projection: "connector-pipe"}`. The connector
  registers no call state because it owns none; the `calls` array and the `projection`
  note are the stable face a desktop builds against if a later batch ever gives the
  connector a real registry to project (the projection note names the actual state
  owner class so the answer cannot be mistaken for a lie of omission).
- `call.incoming` event: the inbound half of the pipe. The engine's `callMessage`
  envelope projection routes through the same body-less control plane as typing and
  reactions (conversation resolved via the shared (account, kind, peer) ladder,
  unresolvable signaling dropped silently), and lands as one transient
  `HostEvent::new("call.incoming", ...)` carrying `{accountId, conversationId, callId,
  type, payload}` — `type` is the projected frame family (`offer`/`answer`/`ice`/
  `busy`/`hangup`, or `opaque` when only the modern opaque framing rides; `callId` is 0
  there because the RingRTC payload carries its own identity), `payload` is the
  engine's projected face verbatim (bounded: opaque ≤ 3 MiB chars, legacy ICE list ≤ 64
  frames, out-of-budget or unaddressable faces never route). Nothing persists and no
  row is written, so a missed event is not self-healed — the RingRTC layer re-signals,
  which is the official recovery story for lossy datagram-framed signaling.

Receive-face bounds (engine.rs): `MAX_CALL_ID` = u32::MAX (an engine frame beyond the
face is unaddressable and routes nothing), `MAX_CALL_OPAQUE_CHARS` = 3,145,728,
`MAX_CALL_ICE_FRAMES` = 64. Engine structured errors need none new: the send face's
parameter/refusal ladder is local, and engine `-32603` parameter faces fold into
`UPSTREAM_ERROR` through the existing fallback (`RUNTIME_NOT_RUNNING` /
`CAPABILITY_UNAVAILABLE` / `SEND_OUTCOME_UNKNOWN` per the §4.32/反应类 discipline).

Capability tag: `call-signaling` joins `FEATURE_CAPABILITIES` and the handshake
capability assertion; desktops gate both methods and the event on it. Local validation
ladder: `accountId` through the opaque-id validator and resolved to the owning proxy
group; `conversationId` bounded and resolved; `type` against the five-value enum;
`callId` ≥ 1; `payload` non-empty and bounded (sendSignal). Persistence and cross-face
boundaries: no row, no cache, no store schema change; the signaling material exists only
inside the engine, the response, and the event, and never reaches a log line (the
AGENTS.md redaction boundary; neither path logs payload content).

#### 4.49.1 Legacy bare frames + urgency pass-through (contract revision 1.53, 2026-10-06)

The P1 real-device experiment closed the open question this connector's §4.49 batch had
left implicit: **RingRTC 2.72.1 emits outbound hangup/busy frames with no opaque
material** (the legacy five-value frame family), while the engine's `sendCallMessage`
face accepted only base64(opaque) — so hangup/busy signaling could not leave through the
pipe at all. Evidence anchors (all read from pinned upstream trees, behavior alignment
only, zero source copied): the vendored libsignal-service `SignalService.proto`
(`CallMessage.Busy{id}` field 5, `CallMessage.Hangup{id,type,deviceId}` field 7, the
envelope-level `opaque` a separate field 10); Signal-Desktop `callingMessageToProto`
(busy={id}, hangup={id,type} — no opaque material on either); signal-cli 0.14.7
`CallManager.sendBusyViaSignal`/`sendHangupViaSignal` (the same bare-frame family). The
engine batch (kt-signal-engine, `docs/jsonrpc-compat.md` §5 legacy 裸帧族 + the
`sendCallMessage` row) builds those frames natively; this batch opens the connector's
send face to them.

- `calls.sendSignal` (1.53): `payload` is now **mandatory for offer/answer/ice** (the
  modern faces carry RingRTC-serialized opaque bytes — unchanged) and **optional for
  busy/hangup**: omitting it routes the engine's `legacy` face — upstream
  `{account, recipient, legacy: {kind: "busy"|"hangup", id: callId}}`, the official
  bare-frame family built engine-side from the addressed `callId`, no bytes invented.
  A *present* payload on a busy/hangup keeps the modern `data` route verbatim (the
  connector never inspects content, so an explicit payload forwards as-is; the
  engine wraps it in the opaque envelope). A payload-less offer/answer/ice refuses
  `INVALID_REQUEST` locally before any upstream call. `required` drops `payload`;
  `urgency` (new, optional, `droppable|handleImmediately`) is the pass-through
  wake-up bit (the P1-遗留② cross-repo gap this batch also closes): forwarded to the
  engine's `CallMessage{opaque{urgency}}`; absent = the engine's droppable default;
  an illegal value refuses locally. Legacy bare frames carry no opaque envelope, so
  urgency is meaningless there and the connector forwards only what the caller sent.
- Schema: `callsSendSignalParams.required` = `[accountId, conversationId, type,
  callId]`; `payload` loses its `required` membership (constraints unchanged);
  `urgency` joins `properties`. Capability `call-signaling` unchanged (a face
  extension of the same pipe, not a new capability — the desktop consume batch needs
  no capability gate change).
- The inbound face is untouched: legacy busy/hangup frames already projected through
  `normalized_call_message` (the §4.49 receive ladder) and land as `call.incoming`
  events with the frame object verbatim.
- Tests: the round-trip drill now covers a payload-less busy and hangup (the `legacy`
  upstream shape asserted on the fixture send log, urgency pass-through both faces),
  a payload-less offer refusal, and an illegal urgency refusal; the unit gate covers
  the same face split, the `legacy` projection, and the urgency enum.

#### 4.49.2 Group signaling: the call.groupIncoming event + the calls.sendSignal group route (contract revision 1.82, 2026-10-08)

The P4b group-call batch (blueprint `kt-signal-calling/docs/group-call-surface.md`
§3.2 — the 810 调研 with its 风险① judgment — and the relay contract v2 draft
`kt-signal-calling/docs/relay-api-v2.md` §3, which pins this event as the shell's
translation source). Official anchors (Signal-Desktop `v8.31.0-alpha.1`, AGPL —
behavior and line anchors only, zero source copied): outbound group signaling is
`#handleSendCallMessageToGroup` (`calling.preload.ts:3338-3407`) landing in the
sender-key fan-out `sendContentMessageToGroup`
(`sendCallingMessage.preload.ts:99-118`); inbound signaling rides the same
`Content.callMessage` envelope whose `CallMessage` proto carries **no group
identity** — RingRTC self-routes the opaque bytes by the group identity embedded
inside them, so at the envelope level a group frame resolves to nothing but its
sender.

- Why the inbound discipline had to move (the blueprint's 风险①): the 1:1 face's
  「(account, kind, peer) ladder resolution failure → silent drop」 (§4.49) is
  fatal for group signaling — a group frame resolves only its sender, and that
  sender's direct conversation usually does not exist, so every group frame died
  on the ladder and group signaling inbound was a total loss. The revision is the
  blueprint's 方案 A: **envelope-level opaque signaling that addresses nothing
  local forwards as the new transient `call.groupIncoming` event** with shape
  `{accountId, sender, payload, urgency, destinationDeviceId?}` — deliberately no
  `conversationId` (honest: the envelope carries none, and inventing one is the
  lie only the resolved 1:1 face can afford), `payload` the opaque base64 under
  the same 3 MiB-char receive budget, `sender` the envelope source the ladder
  resolved (E.164 on a contacts hit, ACI otherwise), `urgency` the projected
  wake-up bit, `destinationDeviceId` present only when the engine projected one.
  Resolved 1:1 frames keep `call.incoming` byte-identical (the P2/P3 consumers
  are untouched); the legacy frame families (offer/answer/ice/busy/hangup) keep
  the silent drop when unresolvable — the proto gives them no group leg, there is
  nothing honest to forward; a face missing its opaque material routes nothing
  (宁缺勿错). Transient like `call.incoming`: no row, nothing persists, a missed
  delivery is not self-healed (the RingRTC layer re-signals). The shell
  translates the event into the relay v2 `call.groupSignal` inbound frame
  `{conversationKey, sender, opaqueB64, urgency}` (relay-api-v2.md §2) and never
  parses the bytes — the desktop-shell discipline is unchanged.
- The send face: `calls.sendSignal` routes by the resolved conversation kind, no
  new parameter (the connector already holds the kind). A **group conversation**
  forwards as the engine group face `{account, group: <master key>, data,
  urgency?}` — `group` is the row's peer key, the same standard-base64 master key
  `listGroups` addresses, and the landing is the engine's
  `send_message_to_group` sender-key fan-out, the official counterpart mirrored
  as a pass-through. A **direct conversation** keeps the current
  `{account, recipient, data|legacy}` verbatim. The payload-less legacy family
  refuses `INVALID_REQUEST` locally on the group route before any upstream call
  (official group signaling rides the opaque envelope exclusively — no precedent
  for group-addressed bare frames, fail-closed). `callId` and `type` gate but
  never travel (unchanged — the RingRTC payload carries its own identity), and
  `urgency` rides through exactly as on the 1:1 face. Send lane,
  account-scoped mutating unchanged: the group fan-out rides the same
  write-lane mutex, so its fan-out latency is the §4.49 known-limitation class —
  recorded here as the batch's latency characteristic. `overrideRecipients` (the
  official fourth parameter, the partial-send face) has no connector face and
  the engine group arm refuses it fail-closed — nothing to gate connector-side,
  recorded honestly.
- Capability: no new bit — `call-signaling` widened in place (the §4.49.1
  discipline: a face extension of the same pipe). Schema: the event enum grows
  `call.groupIncoming` with its def (required `accountId`/`sender`/`payload`/
  `urgency`, optional `destinationDeviceId`), the `call.incoming` description
  notes the revised unresolvable routing, and the `calls.sendSignal`
  main/`conversationId`/`type` descriptions record the kind-split route. No
  method enum, params-shape, or error-code change.
- Engine compatibility (recorded, not worked around — cross-repo boundary): the
  current engine `sendCallMessage` face is 1:1-only and fails closed on the
  group shape (`docs/jsonrpc-compat.md` §3); the group arm (`group`/`recipient`
  mutual exclusion over `send_message_to_group`) is the in-flight engine batch
  of the same P4b wave. Until it lands, a group `calls.sendSignal` surfaces the
  engine's refusal verbatim as `UPSTREAM_ERROR` — the connector invents no
  success. The local fixture mirrors the negotiated wire face (group+recipient
  mutual exclusion, `{timestamp}` on success) so the connector tests pin the
  route, not the engine's interim refusal.
- Alignment points left open: `call.groupRingUpdate` is named by relay-api-v2.md
  §3 as this event's ring-batch counterpart and is deliberately absent here —
  `RingUpdate` is derived inside the RingRTC layer from the opaque bytes plus
  local state, which the connector neither runs nor parses; it lands with the
  batch that owns that derivation. The relay v2 protocol-version gate and the
  `call.config`/`groupControl` frame family are calling-repo faces.
- Tests: the unit gate covers the group send route (upstream `{account, group,
  data}`, `urgency` pass-through, no `recipient`, no traveling `callId`), the
  group legacy refusal, the group modern-face busy-with-payload pass-through,
  and the unresolvable-opaque routing (`sender`/`payload`/`urgency`/
  `destinationDeviceId` carried, no row persisted; legacy and malformed faces
  drop; a source-less face drops); the integration drill extends the signaling
  round trip with a group send (the send log asserts the group face and the
  absence of a `recipient` key) and an unresolvable opaque envelope (the
  `call.groupIncoming` event arrives with no `conversationId` key and no
  `call.incoming` fires).

### 4.50 Group termination: groups.terminate (contract revision 1.52, 2026-10-06)

The desktop matrix item 「终止群（isGroupTerminated）待协议核实」 closes as **verified with
an upstream face**: the official desktop's conversation-info page does carry the action
(`ConversationDetailsActions.dom.tsx` L144-154, the "End group" destructive item), and the
pinned signal-cli 0.14.8 JSON-RPC surface carries the matching write natively — no
invention needed, this batch is behavior alignment with zero upstream source copied.

Verification record (2026-10-06, all line anchors read from the pinned upstream trees):

- Official behavior anchor (Signal-Desktop `v8.31.0-alpha.1`, AGPL — semantics only): the
  action is `onTerminateGroup` → the `terminateGroup` duck (`state/ducks/conversations.preload.ts`
  L3105, wrapped in a `longRunningTaskWrapper` with the `icu:GroupV2--terminate-group-in-progress`
  spinner, failures route to the `TerminateGroupFailedModal` retry dialog) → the model's
  `terminateGroup()` (`models/conversations.preload.ts` L5131, GroupV2-only) →
  `buildTerminateChange` (`groups.preload.ts` L1472) — **a GroupsV2 group change, not a
  dedicated RPC**: `{version: revision+1, terminateGroup: {}}` uploaded through
  `modifyGroupV2` → `WebAPI.modifyGroup` (`textsecure/WebAPI.preload.ts` L4919, the zkgroup
  credential-authenticated `PATCH /v1/groups` protobuf call). Availability gate:
  `canTerminateGroup = isTerminateGroupEnabled && !isGroupTerminated && isAdmin`
  (`ConversationDetails.dom.tsx` L256, behind the server-driven featured flags
  `desktop.groupTerminate.send.beta` / `.prod`). Post-termination official state: the
  composer is blocked (`icu:CompositionArea--group-terminated`), leave/block hide, archive
  + delete show, members render as former members, and a terminated group's inbound
  messages are dropped (`icu:GroupV2--terminated--you|other|unknown` timeline copy; the
  terminated flag can only ever move false→true, `groups.preload.ts` L5008).
- Upstream face (signal-cli `v0.14.8`, the pinned engine baseline — introduced upstream in
  `13819c8f` 2026-08-08 "Support the GroupsV2 end group (terminate) feature" #2098): the
  JSON-RPC local command `terminateGroup` (`TerminateGroupCommand.java`, dests
  `-g/--group-id/--group` + the standard `account`) → `ManagerImpl.terminateGroup` L614 →
  `GroupHelper.terminateGroup` L366 (GroupV2-only — a v1 group answers the
  "only supported for Signal group v2 groups" IO error; a revision `ConflictException`
  refreshes the group and retries once *inside the engine*, upstream-owned semantics the
  connector neither sees nor repeats) → `GroupV2Helper.terminateGroup` L546 → libsignal
  `createTerminateGroup()`; the result is the send fan-out (`SendGroupMessageResults`,
  same shape family as `updateGroup`/`quitGroup`). Error faces: non-admin termination is
  refused by the GroupsV2 service (the `GroupSendingNotAllowedException` user-error
  family), unknown/departed groups answer `GroupNotFoundException` /
  `NotAGroupMemberException` — all forward verbatim as `UPSTREAM_ERROR`, never invented
  locally. Reading the terminated state: `listGroups` JSON carries `isTerminated`
  (`ListGroupsCommand.java` JsonGroup L168), and the engine refuses sends to a terminated
  group locally (`SendHelper` L578) and drops inbound messages addressed to one
  (`IncomingMessageHandler` L839).
- presage/libsignal-service stack: **no separate RPC exists at any layer — by protocol
  design**. Termination is a GroupsV2 group-change action (`GroupChange.Actions.terminateGroup`),
  so the only "wrapper" is a generic group-change builder. The connector's upstream is
  signal-cli (JVM/native) which now builds that action natively; no presage face is needed
  or consulted (presage is not in this repository's dependency tree — recorded so the
  matrix item does not reopen).
- kt-engine mode: the engine face has no `terminateGroup` outbound method (the same
  engine-batch debt `groups.quit` carries), so engine mode answers
  `CAPABILITY_UNAVAILABLE` (`retryable=false`) after local validation — the §4.44
  media-ingest downgrade precedent. The engine batch removes the gate without a connector
  contract change.

- `groups.terminate` (host method, capability `group-terminate`, send lane, account-scoped
  mutating): params `{accountId, conversationId, operationId?}` — the groups.quit shape
  verbatim (the action itself carries no parameters, so the change set is empty by
  construction). The group is addressed by conversation id like every other conversation
  face and must resolve to a `kind='group'` row before any upstream call (direct answers
  `INVALID_REQUEST`, unknown answers `CONVERSATION_NOT_FOUND`); `operationId?` is
  validated by shape and never persisted (the §4.5 convention). The upstream payload is
  the pinned dests verbatim: `{"account", "groupId"}` (peer key = the group id).
- Result semantics (the §4.44 mutating discipline): a definite success answers
  `{"status": "sent"}` — the engine's `SendGroupMessageResults` body is not interpreted;
  an indeterminate upstream outcome (engine exit or timeout with the call in flight)
  answers the `SEND_OUTCOME_UNKNOWN` error, never retried by the connector. Zero local
  writes: the terminated state converges through the §4.44 group-update receive
  projection when the change's own envelope lands, and after termination the engine
  refuses/drops that group's traffic upstream (the honest near-silence the desktop
  renders from the terminated banner — desktop presentation is the consume batch's).
- Read-face boundary (recorded, deliberately not widened in this batch): the connector's
  `groups.get` cache row (§4.8/§4.39) still carries `memberCount` + `members` only — the
  sync's `group_extra_json` does not capture the upstream `isTerminated` flag yet, and the
  §4.44 receive envelope (`JsonGroupInfo`) exposes no terminated field. A desktop that
  needs the terminated marker reads the next consume batch's projection work; this batch
  ships the write face only, so the matrix item's protocol question is closed without
  inventing a read surface the desktop has not asked the connector for.
- Capability tag: `group-terminate` joins `FEATURE_CAPABILITIES`; the desktop consume
  batch gates the method on it (a pre-1.52 connector answers a clean capability gap
  instead of `METHOD_NOT_ALLOWED`). Schema: the method enum, the `groupsTerminateParams`
  def, and the allOf dispatch branch land together with the code.

> **Dated appendix (2026-10-06, group-terminate consume batch, contract revision 1.53):
> the read face opens.** The read-face boundary above is lifted for the sync path: the
> contacts sync now captures the upstream `JsonGroup.isTerminated` flag it already reads
> (`ListGroupsCommand.java` JsonGroup L168, the §4.50 verification) into the cached group
> row's `group_extra_json` — stored only when true, the same informative-keys discipline
> as the roster's `admin` mark — and `groups.get` projects it as a plain bool
> `isTerminated` (always present; false for every pre-1.53 row and every engine whose
> `listGroups` face carries no flag). The receive-side half of the boundary stays exactly
> as recorded: the §4.44 receive envelope (`JsonGroupInfo`) exposes no terminated field
> and the engine's `groupUpdated` projection names timer fields only, so there is no
> honest receive-chain source for the flag — the projection cannot see a termination
> happen, it can only read the state the upstream itself exposes, which is `listGroups`.
> Consequences recorded honestly: a group this account terminates answers `{"status":
> "sent"}` while its cached row still reads `isTerminated: false` until the next
> `contacts.sync` (the flag is as fresh as the sync — the §4.44 staleness boundary for
> the whole cache row, `syncedAt` remains the signal; the desktop consumes the flag with
> its own sync-on-open rhythm, the same freshness contract `memberCount`/`members`
> already carry). The flag's lifetime follows the row, not the action: rows are rewritten
> wholesale at each sync from what the upstream answers, so no connector-side latch is
> added and the terminated flag can only change at a sync boundary. No
> schema shape change beyond the one result key, no new method, no new capability: the
> read face rides `groups.get` (an ungated pre-capability read method) and the
> `group-terminate` tag the write face already carries — one capability for one protocol
> action, its read and write halves together. Tests: the sync-capture unit faces
> (`group_extra_json` true/false/absent), the projection unit face (pre-1.53 row false,
> flagged row true, wire bool on both), and two server integrations (the default
> fixture row answers false; the `KT_FAKE_TERMINATED_GROUPS` stub hook answers true
> through the full listGroups → cache → groups.get ladder).

### 4.51 Link-time import progress + manual retry: `history.importStatus.progress`, `history.importRetry` (contract revision 1.54, 2026-10-06)

The S4 UI gap the desktop batch named against the official import screen: the official
Download stage renders a real "X of Y + %" byte progress (`icu:BackupImportScreen__progressbar-hint`
with `currentSize`/`totalSize`, the reducer payload `{backupStep, currentBytes, totalBytes}`
in `ts/state/ducks/installer.preload.ts` L143-152, applied at L642-675), and the official
failure dialog carries a manual Retry (`retryBackupImport` L495-505 →
`backupsService.retryDownload()`, the `InstallScreenBackupError.Retriable` dialog's confirm
button in `InstallScreenBackupImportStep.dom.tsx` L107-135). The connector's 1.39 face had
neither: `history.importStatus` was state + counters only, and a failed import waited for
the next connector start (the ≤3-attempt sweep) with no desktop trigger. This revision
closes both gaps without touching the ledger's honesty rules. Official source is read for
behavior semantics and anchors only (AGPL, `/Users/tanye/test/Signal-Desktop-src`,
zero copy).

- `progress` object (contract shape, byte-for-byte as the desktop batch pins it):
  `{stage: "downloading"|"processing", bytesTransferred?, bytesTotal?}`, present only while
  `state === "running"`. `downloading` is the engine's transfer-archive byte snapshot: the
  connector queries the kt-engine's `transfer.downloadStatus` (one bounded read-only call,
  new kt-signal-engine extension — the engine's per-account in-process snapshot registry,
  no files, no disk persistence; engine crash/restart reads as absent). The byte fields
  ride together or not at all: the engine reports both only when it read a content-length;
  otherwise `stage` alone — never a zero-filled total (the official UI keeps a spinning
  bar when the server sends no length; we render stage copy the desktop side).
  `processing` is the connector's own NDJSON import pass (a `running` row with no engine
  download snapshot) and never carries byte fields — the official Process stage renders
  copy without numbers (`InstallScreenBackupImportStep.dom.tsx` L287-296).
  `pending`/`completed`/`failed`/`unavailable` never carry `progress`. Shape gate: the
  snapshot is engine output, not trusted input — `present` must be a true boolean, byte
  fields non-negative integers within u64, `bytesTransferred ≤ bytesTotal` when both ride;
  a malformed snapshot drops the `progress` object and keeps the state (better to omit
  than to get it wrong). JVM/native modes keep answering the plain shape (no engine call
  is possible there).
- `history.importRetry` (host method, capability `history-import` — widened in place, no
  new bit: a 1.39-aware desktop keeps gating on the same tag): params
  `{accountId}`; result `{armed: true, state: "pending"}`. Semantics: clear the account's
  `failed` ledger row and re-arm the orchestration — with the archive on the engine face
  the state returns to `pending` and the sweep (next engine start) or the link watcher
  picks it up; `begin_history_import` re-inserts the row with `attempts + 1` (the ≤3
  automatic budget stays supervisor-owned; the manual face is unbounded like the official
  dialog). Error ladder: shape errors `INVALID_REQUEST` (retryable=false, the params-gate
  arm); a ledger row that is not `failed` (pending/running/completed) or a JVM/native
  face `NOT_RETRYABLE` (retryable=false — completed is the consumption marker, running
  belongs to the live pass, pending waits inside its own window); no importable archive
  on the engine face (a failed row whose archive vanished, or no row at all with no
  archive) `NO_ARCHIVE` (retryable=false). Unknown account: `ACCOUNT_NOT_FOUND` (the
  shared store path).
  Send lane, account-scoped mutating — the reaction-class write-lane mutex and delete
  drain barrier serialize it against every other account mutation (the `backups.export`
  class; the store delete is the only local write). Metrics class `accounts`. No upstream
  traffic, no engine call in the retry itself; the re-armed import consumes the same
  bounded orchestration as the link-time flow.
- Store: `clear_failed_history_import` — `DELETE FROM history_imports WHERE account_id=?
  AND state='failed'`; the caller owns the state gate, and a 0-changed-rows delete is a
  benign same-account race (the row is already gone). Schema version unchanged (the
  1.39 table already carries every column the retry needs).
- The engine face (`transfer.downloadStatus`) is a kt-signal-engine extension registered
  in the engine's own compat doc (`docs/jsonrpc-compat.md` §3) — the connector treats it
  as an untrusted projection source and keeps working when the engine predates it (the
  call fails or answers a foreign shape → no `progress`, the pre-1.53 answer byte-for-byte).

### 4.52 Conversation-summary / contact-row terminated projection (contract revision 1.60, 2026-10-06)

The desktop's remaining §14.2.1 gap (projection difference ④) closes with data: the
official forward-target list excludes terminated groups (`selectors/conversations.dom.ts`
L709 `canComposeConversation` / L725 `getAllComposableConversations`, both gated on
`!conversation.terminated`; the forward modal consumes the latter through
`ForwardMessagesModal.preload.tsx:66`). The 1.53 §4.50 appendix already gave the sync the
capture (`group_extra_json` stores the upstream `JsonGroup.isTerminated` when true) and
`groups.get` the single-group read — but the forward dialog renders the whole conversation
list, and no batch group-state read face exists (an N× `groups.get` storm, or filtering
only the LRU-cached subset, would be dishonest projections; recorded 2026-10-06). This
revision widens the existing summary projection faces instead of inventing a new one —
the flag moves to where the desktop already reads every conversation.

- Capture stays exactly the §4.50 appendix facts: only the contacts sync writes the flag
  (when the upstream `listGroups` JsonGroup carries `isTerminated: true`), stored in the
  `kind='group'` row's `group_extra_json`, rewritten wholesale at each sync, refreshable
  only at a sync boundary (`syncedAt` remains the only staleness signal). Nothing in this
  batch writes or locally derives the flag — no receive-chain source exists (the
  `JsonGroupInfo` envelope exposes no terminated field), the same honest boundary 1.53
  recorded.
- Projection faces (read time, zero new storage, no schema change — schema stays 18):
  - `conversations.list` items carry `isTerminated` as a plain bool — present only when
    true (collapsed to an absent key when false, the same informative-keys discipline as
    the roster `admin`/`self` marks), so pre-1.60 hosts read byte-identical summaries.
    The flags are stamped after the page query through one contacts-cache lookup (the
    group-kind rows; for a group the conversation row and the contacts row share the same
    `stable_hash_id([account, 'group', peer])` id, so the stamp joins on id; a group with
    no cache row yet stays false).
  - The single-row summary that rides `conversation.changed` carries the same flag (the
    same id-keyed read through the already-held connection guard), so an event-driven
    window and a full reload cannot disagree about one conversation.
  - `contacts.list` rows carry the same `isTerminated` (their own row's marker for
    `kind='group'`; direct rows and pre-1.53 sync rows always false/absent) — the compose
    face and the conversation face cannot disagree about the same group.
- Consumers: the desktop forward-target filter (matrix §14.2.1 ④, closes ✅ with data =
  contract 1.60). The `groups.get` shape is untouched — it already answered the plain
  bool `isTerminated` since 1.53.
- Tests: the store projection faces (terminated group true on both the list and the
  single-summary path, non-terminated/pre-1.53 rows absent-key false byte-identical,
  direct rows never marked), the `contacts.list` row face (group true / group false /
  direct absent), and the existing 1.53 sync-capture + `groups.get` faces stay green
  (no capture-path change, zero retirement).

### 4.53 Group join-request approval: groups.approveJoinRequests (contract revision 1.58, 2026-10-06; §4.52→§4.53 renumbered on landing — 1.60 §4.52 claimed the section number first)

The group-management matrix item 「群加入审批（approve/joinRequest）」 closes as a **safe
managed write over an upstream face the pinned engine already carries natively** — no
upstream source copied, no protocol invention.

Verification record (2026-10-06, all line anchors read from the pinned upstream trees):

- Upstream face (signal-cli `v0.14.7-4-g472784e6` checkout, the 0.14.8 line — semantics
  verified in the pinned tree): approval is **woven into the updateGroup member semantics**.
  `GroupHelper.updateGroupV2` (lib/.../helper/GroupHelper.java:730-733, 769): `members`
  entries that are requesting members ride `GroupV2Helper.approveJoinRequestMembers`
  (GroupV2Helper.java:297), `removeMembers` entries that are requesting members ride
  `refuseJoinRequestMembers` (:309) — every other entry is an ordinary add/remove member
  change. The read source is `listGroups`: `ListGroupsCommand` projects
  `requestingMembers` as `JsonGroupMemberAddress {number?, uuid?}` records
  (ListGroupsCommand.java:127, the NON_NULL omissions a single-identity candidate
  carries). The underlying protocol actions are the GroupsV2
  promote/delete `pending_admin_approval` pair — server-adjudicated (admin-only, 403),
  revision-Conflict retried inside the engine (`GroupHelper.updateGroup` ConflictException
  → refresh + one retry, upstream-owned semantics the connector neither sees nor repeats).
- The hazard this batch exists for: the shared `members`/`removeMembers` arrays mean a
  naive "approve these people" call **adds non-requesting names to the group** (and a
  naive refuse **removes existing members**). The official client avoids it by comparing
  against `requestingMembers` first; the connector does the same, server-side of the API
  boundary.
- `groups.approveJoinRequests` (host method, capability `group-join-approve`, send lane,
  account-scoped mutating): params `{accountId, conversationId, members, approve,
  operationId?}` — `members` is the candidate set (1-1001 entries of 1-128 characters,
  the §4.39 roster bounds; an empty array is refused, the §4.44 no-delta discipline),
  `approve: true` approves / `false` refuses. conversationId must resolve to a
  `kind='group'` row before any engine call (direct answers `INVALID_REQUEST`, unknown
  answers `CONVERSATION_NOT_FOUND`); duplicated candidates collapse.
- The fold (supervisor-side, the safety core): before anything goes upstream the
  connector reads the engine's own `listGroups {account, groupId}` (single-group read,
  ReadOnly class) and intersects the candidates with the projected requesting
  identities (number and uuid both ride one identity set). Only requesting survivors
  can ever ride the upstream `members` (approve) / `removeMembers` (refuse) array —
  every other change key stays absent (§4.44: absence = no change). A fold that
  empties the candidate set answers `JOIN_REQUEST_NO_LONGER_PENDING` (retryable —
  the request was resolved elsewhere: another admin acted, the candidate withdrew)
  and **no write is dispatched**. A read face that cannot answer fails closed with
  its own engine error. Mis-targets therefore fail at the connector, never at the
  group's member list.
- Result semantics (the groups.terminate discipline verbatim): a definite success
  answers `{"status": "sent"}` (the engine's SendGroupMessageResults body is not
  interpreted); an indeterminate upstream outcome (engine exit or timeout with the
  write in flight) answers `SEND_OUTCOME_UNKNOWN`, never retried. Zero local writes:
  the roster converges through `contacts.sync` (the requesting set is as fresh as the
  sync the fold's own read refreshes upstream-side; the cached row keeps its own
  §4.50-appendix-era staleness boundary).
- Error ladder: shape/resolution errors `INVALID_REQUEST` / `CONVERSATION_NOT_FOUND`
  (retryable=false, before any engine call); upstream non-admin (GroupsV2 UNAUTHORIZED)
  forwards verbatim as `UPSTREAM_ERROR` (retryable=true, the §4.50 family); the
  stale-fold `JOIN_REQUEST_NO_LONGER_PENDING` (retryable=true); unknown outcome
  `SEND_OUTCOME_UNKNOWN` (retryable=false, never retried). The kt-engine face has no
  updateGroup member-family method (the engine's updateGroup takes the expiration family
  only — its compat §5 batch boundary) and answers `CAPABILITY_UNAVAILABLE`
  (retryable=false) after local validation — the groups.update/groups.quit engine-batch
  debt precedent, honest by design: answering the fold instead would disguise "the
  engine cannot approve" as "nobody is requesting".
- Capability tag: `group-join-approve` joins `FEATURE_CAPABILITIES`; the desktop
  consume batch gates the method on it (a pre-1.58 connector answers a clean
  capability gap instead of `METHOD_NOT_ALLOWED`). Desktop UI rendering (approve /
  refuse actions on the requesting-members list) is the follow-up consume batch; this
  batch ships the connector face + handshake declaration only. Schema: the method
  enum entry, the `groupsApproveJoinRequestsParams` def, the allOf dispatch branch,
  the `JOIN_REQUEST_NO_LONGER_PENDING` error-code entry, and the roster-bounds mirror
  rows in the schema-consistency test land together with the code.
- Tests: the prepare unit face (happy path both directions + duplicate collapse; the
  ladder — empty members, oversized array, per-entry shape, unknown conversation,
  direct addressing, over-bound audit id; the engine-mode CAPABILITY_UNAVAILABLE after
  validation, and INVALID_REQUEST precedence on the engine face), the fold unit face
  (dual-identity capture, malformed entry drops, foreign-shape fail-closed, survivor
  order), and the server integration (handshake advertises the method + tag; the fold
  keeps only the requesting survivor in the recorded upstream payload; the refuse
  direction rides `removeMembers`; the stale fold answers
  JOIN_REQUEST_NO_LONGER_PENDING with zero writes; unknown conversation / direct /
  unknown-params-key / empty-members answer before any write; the upstream refusal
  forwards UPSTREAM_ERROR; the in-flight crash answers SEND_OUTCOME_UNKNOWN and is
  never retried across the restart). Fixture: `KT_FAKE_REQUESTING_GROUPS`
  (`<groupId>=<addr,addr>;<groupId>=…`) rides the listGroups JsonGroup with the
  `requestingMembers` array; the updateGroup route grows the `[fixture-approve-forbidden]`
  user-error sentinel and the `[fixture-approve-crash]` `os._exit(36)` sentinel.

### 4.54 Live message-attachment pointer persistence + on-demand download: `attachments.download` (contract revision 1.61, 2026-10-06; §4.52→§4.54 renumbered on landing — 1.60 §4.52 and 1.58 §4.53 claimed the lower numbers first)

The message-attachment on-demand batch: the desktop's attachment pills (image thumbnails,
voice notes) need a byte channel keyed to **live** message rows, not only the link-time
import rows §4.38 step 3 serves. Two connector halves land together.

- Receive locator persistence (store schema 18 → 19): the receive envelope's attachment
  objects may carry the §4.38 pointer face under a `locator` key (the engine-side
  projection the archive export already ships). The engine normalization captures it under
  the 1.56 ingest caps verbatim (key/digest/cdnKey base64 ≤512 chars, plaintextHash hex
  ≤128 chars lowercased, cdnNumber ≤u32::MAX, uploadTimestamp > 0; wrong-shaped keys drop;
  an all-empty face collapses to the hole) into a new `NormalizedAttachment.locator`
  field, and `insert_message` serializes the index-aligned faces into the new internal
  `messages.live_attachment_locators` column. The host wire stays byte-identical (§4.13):
  key material never crosses it, and the descriptor shape is unchanged. Facts recorded
  honestly: the pinned signal-cli 0.14.8 receive envelope (`JsonAttachment`) and the
  kt-engine receive projection (`project_attachment_pointer`) ship metadata only today, so
  live rows store the all-None faces (indistinguishable from NULL on the read face) until
  an engine ships the receive projection — the column then fills without a further
  migration, and `attachments.download` becomes live for new messages automatically.
  Additive nullable, never backfilled.
- `attachments.download` (Send lane, account-scoped mutating, capability
  `attachment-download`): params `{accountId, conversationId, messageId, attachmentIndex}`
  — positional addressing, because the synthetic descriptor ids an engine face emits are
  not stable addressing (the §4.54 ruling); the index is capped at the 32-descriptor
  receive ceiling and must name a descriptor of the message. The pointer face resolves
  through the locator ladder: the live column first (schema 19), then the schema-18 import
  column (the index-aligned archive faces, already persisted since 1.56) — a hole in both
  answers `INVALID_REQUEST` before any network traffic, never a guessed download.
  Everything after the staging is the §4.38 step-3 machine verbatim: same
  fail-closed `TransitFace` decode, same CDN stream/gates/decrypt/trim/hash chain, same
  governed media-directory landing under the descriptor's sanitized id, same idempotent
  already-present dedup, same error ladder (terminal
  download/verify failures `UPSTREAM_ERROR retryable=false`; local-IO
  `retryable=true`). Result `{sizeBytes}` — afterwards
  `messages.attachments.open/readChunk` deliver the bytes exactly like any
  engine-downloaded file. Dispatch is the `history.attachmentDownload` class: the reaction
  write-lane mutex and delete drain barrier; metrics class `send`.
- Desktop gating: capability `attachment-download` (new tag; the §4.38 faces keep their
  no-new-bit `history-import` discipline, but this face's audience is every
  attachment-pill desktop, not only the history-import ones). A desktop without the tag
  keeps the metadata-only rendering (zero calls); contract 1.61 wire consumers are
  unaffected. Contract 1.98 makes the advertisement honest: the handshake carries the tag
  only when the runtime's media ingest is armed — the exact gate this face answers
  (`CAPABILITY_UNAVAILABLE` without it, the registry's kt-engine downgrade included) — so
  the packaged shape (ingest off) omits the tag instead of inviting a call that fails for
  every attachment.

### 4.55 Signal username: `accounts.setUsername`, `accounts.getUsername`, `username.lookup` (contract revision 1.59, 2026-10-06; §4.52→§4.55 renumbered on landing — §4.52-§4.54 claimed by the 1.60/1.58/1.61 batches)

The Signal username capability for the KT chain: an account can set a contactable username
(what other people reach it by without a phone number), delete it, and resolve other
people's usernames/username links to registration status. The upstream anchors in the
pinned signal-cli 0.14.8 are `UpdateAccountCommand` (the `updateAccount` jsonRpc method,
`username` XOR `delete-username` mutually exclusive group at UpdateAccountCommand.java:41-46,
`InvalidUsernameException` → UserErrorException → JSON-RPC error "Invalid username: ..."),
`ManagerImpl.setUsername` (a value containing `.` routes to `reserveExactUsername`, a bare
nickname to `reserveUsernameFromNickname` which lets the server pick the discriminator),
`AccountHelper` (USERNAME_MIN_LENGTH 3 / USERNAME_MAX_LENGTH 32, the libsignal
`Username.candidatesFrom` denominators), and `GetUserStatusCommand` (`--username` nargs=*,
the `[{recipient, number?, username?, uuid?, isRegistered}]` array). Official desktop
source is read for the grammar anchors only (AGPL, zero copy): `ts/util/Username.dom.ts`
pins nickname `[a-zA-Z_][a-zA-Z0-9_]*` plus an optional all-digits discriminator with
libsignal's 0..65535 range and no-leading-zero forms (a `0`-leading discriminator is
exactly two digits).

- `accounts.setUsername` params `{accountId, username?, deleteUsername}` — XOR enforced in
  the service ladder (both present or both absent → `INVALID_REQUEST`, retryable=false;
  the schema cannot express XOR so both stay optional). The username shape gate is the
  desktop grammar mirrored fail-closed (3..32 chars total; nickname charset; `.N`
  discriminator all digits, no `00` prefix, `0`-leading only as exactly two digits,
  0..65535); a bare nickname is allowed (server picks the discriminator). The server
  reserve/confirm stays the authority — a shape-valid but taken username is the upstream
  "Invalid username: ..." user error, surfaced as `UPSTREAM_ERROR` (no new structured
  code; the `-1` upstream code never crosses the host boundary verbatim).
  Send lane, class `accounts`, account-scoped mutating (`CallClass::Mutating`): an
  indeterminate outcome answers `SEND_OUTCOME_UNKNOWN` and is never retried. Result on
  the set path: `{username, usernameLink?}` straight from the upstream write-back
  (`m.getUsername()` / `m.getUsernameLink()`, NON_NULL); on the delete path the upstream
  writes no result body, so the projection is `{}`. A same-value set is upstream-idempotent
  (`reserveExactUsername` refreshes the current username in place).
- `accounts.getUsername` deliberately answers `CAPABILITY_UNAVAILABLE` (retryable=false)
  in every mode, after the standard account ladder: the pinned signal-cli has **no JSON-RPC
  reader** for the local account's username — `JsonAccountResponse` is a private record
  inside `UpdateAccountCommand` fed by the write-back only, and no list/get account face
  carries the field (verified across the pinned checkout). The method exists as wire
  surface so the desktop can gate and call it once an upstream getter lands; display is
  fed by the `setUsername` write-back mirror instead (the desktop persists the projection
  in its own signalProfileMirror).
- `username.lookup` params `{accountId, usernames[1..20]}` — the upstream takes an open
  array; 20 entries is the KT bound for one host frame (`USERNAME_LOOKUP_MAX_ENTRIES`),
  each entry 1..64 characters (a full username is ≤32; the 64 is a frame bound, not the
  grammar). Entries are usernames or username-link URLs (`resolveRecipientByUsernameOrLink`
  resolves both); bare nicknames are not entries. Read lane (`CallClass::ReadOnly`),
  class `read`. Result: `{results: [{recipient, number?, username?, uuid?, isRegistered}]}`
  projected entry-for-entry from the upstream array — `uuid != null` means the entry
  resolved to a registered account. The upstream CDSI face can rate-limit
  (`RateLimitException`) — surfaced as `UPSTREAM_ERROR` like every upstream user error.
- All three faces carry capability tag `username` (rides the handshake capabilities array;
  desktops gate the new surface on it). JVM/native modes dispatch `updateAccount` /
  `getUserStatus` directly; **the kt-engine face has no username method at all** (verified
  against the engine's method table in `docs/jsonrpc-compat.md` §3) and all three answer
  `CAPABILITY_UNAVAILABLE` (retryable=false) after local validation — the groups.update/
  quit engine-batch debt shape, honest by design.
- Zero new structured error codes (`ENGINE_STRUCTURED_ERROR_CODES` unchanged); the local
  ladder is `INVALID_REQUEST` (shape) → `ACCOUNT_NOT_FOUND` (resolution) →
  `CAPABILITY_UNAVAILABLE` (mode/getter) → upstream errors degrade to plain
  `UPSTREAM_ERROR`. Zero local persistence: no username state is written to the store —
  the desktop's signalProfileMirror is the only local copy, fed from the write-back.

> **Dated appendix (2026-10-06, group-approval consume batch, contract revision
> 1.62): the read face opens.** The desktop consume batch named its one missing
> data face against the official approval surface
> (`PendingInvites.dom.tsx` `MembersPendingAdminApproval` L314-393: requesting
> rows with accept/deny actions; the areWeAdmin gate L351; the
> `getGroupMemberships.dom.ts` L54-63 `pendingApprovalMemberships` join): the
> 1.58 write face folds candidates against the engine's own single-group
> `listGroups` read, but nothing projected that read, so the desktop had no
> list to render. This appendix widens `groups.get` with `requestingMembers` —
> **a live read, deliberately not a `group_extra_json` sync capture**, and the
> reasoning is recorded because the §4.50-appendix precedent cuts the other
> way there:
>
> - **One truth source.** The 1.58 record fixed the approval truth source as
>   the fold's own in-call `listGroups` read ("the requesting set is as fresh
>   as the sync the fold's own read refreshes upstream-side" was the honest
>   note about the *roster*, not about the fold). The read face consumes the
>   same single-group read, so the rendered list and the folded write set are
>   the same bytes moments apart. A sync snapshot would be a second, older
>   truth: the fold would still refuse a candidate the UI swore was
>   requesting (`JOIN_REQUEST_NO_LONGER_PENDING` is exactly that lie made
>   into an error code).
> - **The sync path cannot carry this face.** `contacts.sync` short-circuits
>   at 60s; an approval action writes nothing locally (the 1.58 zero-local-
>   writes discipline), so the "refresh the read after the action" step would
>   answer the pre-action snapshot for up to a minute — the official UI's
>   post-action row disappearance (the requester leaves the list when the
>   change lands) could not converge through it.
> - **The §4.50-appendix precedent is a state bit, not a set.**
>   `isTerminated` is low-frequency and monotone (false→true only), the
>   receive envelope carries no terminated field, and the desktop renders a
>   banner from it — sync freshness is honest there. The requesting set is
>   high-frequency and changes from *any* admin on *any* device, without an
>   envelope the connector can project; the freshness contract that is
>   honest for the flag is stale-by-construction for the set.
>
> Shape: `requestingMembers` rides the `groups.get` result as an optional
> array of `{id, uuid?, name?}` — `id` is the connector-wide number-before-
> uuid address ladder (the 1.58 fold accepts either identity, so the ladder
> id always folds), `uuid` only when the upstream supplied one and it differs
> from the id, `name` resolved at read time from the same account's contacts
> cache the §4.39 roster resolves against (absent when unknown — the desktop
> renders its avatar fallback). Bounded to the §4.39 roster cap (1001
> entries), malformed entries drop individually. **Present only when the
> engine answered a non-empty requesting set**; absent otherwise — engine
> stopped, upstream face missing, or nobody requesting. Pre-1.62 hosts read
> byte-identical responses (the key is absent on every face they knew). The
> capability story is the 1.53-appendix discipline verbatim: no new tag —
> the read rides the `group-join-approve` capability the write face already
> carries, one capability for one protocol action's read and write halves
> (the method itself stays ungated like every §4.8 cache read; the desktop
> consume batch gates the UI on the capability and on this key's presence —
> an old connector answers without the key and the section renders nothing).
> Failure posture: the live read is best-effort by design — an engine that
> is not running or a refused read degrades to the cached row without the
> key (the §4.8 promise "the cached face never fails because of the engine"
> stays intact; the approval section is a privilege, not a load-bearing
> face). Tests: the projection unit faces (requesting entries project with
> ladder id + uuid + read-time name; malformed drop; cap holds; absent on
> None/empty; name resolution joins the contacts cache), the degradation
> unit face (store-only call answers without the key), and the server
> integration (the live key rides a real engine answer through the full
> listGroups → projection ladder; the fold keeps the same survivor set the
> read rendered).
> Consequence recorded honestly: `groups.get` is no longer a pure
> store-only read — it now makes one bounded engine round-trip when an
> engine is running (single-group `listGroups`, ReadOnly class, the same
> traffic the 1.58 fold already spends per approval). A desktop that wants
> the engine-free face can keep reading it while the engine is stopped and
> simply not see the approval section; `syncedAt` remains the freshness
> signal for every other field in the response.

### 4.56 Message-request response faces: conversations.acceptRequest, conversations.declineRequest (contract revision 1.63, 2026-10-06; §4.55→§4.56 renumbered on landing — 1.59 claimed §4.55 first)

The message-requests matrix item (the official Message Requests convergence flow) closes its
data plane as **two thin managed writes over the pinned upstream's native sync face** — no
upstream source copied, no protocol invention, no engine change. The upstream face is
`sendMessageRequestResponse` (pinned checkout `v0.14.7-4-g472784e6`): Rust JSON-RPC
`client/src/jsonrpc.rs:342` (params `account?`, `recipients[]`, `groupIds[]`, `type`) and the
Java local command `src/main/java/.../SendMessageRequestResponseCommand.java:17,51` — the
official CLI type enum carries **only accept and delete** (`client/src/cli.rs:664-669`; the
Java command folds everything else to DELETE), so the official block / block-and-delete /
unblock-and-accept twins (`ManagerImpl.java:1373-1375,1406-1408`, over the internal Manager
API — not JSON-RPC) stay OUT of this contract: they are the desktop's explicit
`conversations.setBlocked` (§4.47) compositions, unchanged. The underlying write is the
multi-device `MessageRequestResponse` sync message (`SyncHelper.java:399-419`), and an accept
additionally flips the contact's profile-sharing bit upstream
(`SyncHelper.java:412-415` — the official acceptConversation semantics).

- `conversations.acceptRequest` / `conversations.declineRequest` (host methods, capability
  `message-requests`, send lane, account-scoped mutating — the reaction-class write-lane
  mutex and delete drain barrier verbatim): params `{accountId, conversationId}` — the same
  addressing pair as the §4.47 faces, the request kind carried by the method choice
  (one button per action, the official desktop's message-request row). Resolution: a direct
  conversation forwards the row's resolved peer reference as `recipient`; a group/legacy
  conversation forwards the row's peer key — the raw group master key, the same `listGroups`
  id the sync materialized — as `groupId` (the upstream takes `groupIds` for groups, never a
  recipient); the §4.42 self row (empty peer reference) and a pre-sync group row fail closed
  before any upstream call (`INVALID_REQUEST`). The connector stamps the upstream `type`
  (`accept` / `delete`) and never composes a second upstream write onto one host method.
- Result `{status}` (`"accepted"` / `"declined"`): the pinned JSON-RPC face answers an empty
  result object on success (the command writes no JSON output,
  `SendMessageRequestResponseCommand.handleCommand`) and folds every per-recipient failure —
  including the unregistered-failure result type (`JsonSendMessageResult.java:33-34,45`) —
  into an unstructured USER_ERROR `-1`, so no per-failure ladder exists to honest-map: a
  USER_ERROR folds into the plain `UPSTREAM_ERROR` like every account-domain method. An
  indeterminate outcome answers `SEND_OUTCOME_UNKNOWN` and is never retried (unknown send
  results are terminal, the AGENTS.md discipline).
- Zero local writes: the request state converges through the receive side — a synced
  `messageRequestResponse` envelope on the account (`ReceiveMessageHandler.java:413-424`
  projects type/groupId/person; the official desktop consumes the same event and stamps
  `messageRequestResponseType` locally, deriving the accepted state with
  `isConversationAccepted.preload.ts:21-82`) stamps the contact row's accepted flag in the
  §4.56 desktop projection below. The connector itself persists nothing.
- Desktop gating: capability `message-requests` (new tag). A desktop without the tag keeps
  its current rendering (zero calls); contract 1.61 wire consumers are unaffected.

### 4.57 Group invites: the `pendingMembers` read face + `groups.revokeInvites` (contract revision 1.64, 2026-10-06; landed after §4.56 on the rebased stack)

> **Upstream evidence (pinned signal-cli `v0.14.7-4-g472784e6`, the 0.14.8
> line — line anchors in the pinned tree):** the read source is
> `ListGroupsCommand`'s JSON projection: `pendingMembers` rides the
> `JsonGroup` record through the same `resolveJsonMembers(group.pendingMembers())`
> call that projects `requestingMembers` (ListGroupsCommand.java:125-133;
> the `GroupInfoV2.getPendingMembers` storage read at GroupInfoV2.java:144-151
> resolves each pending entry's own ServiceId — **no inviter metadata
> survives**: a `JsonGroupMemberAddress` carries `number`/`uuid` only, the
> official `GroupV2PendingMember.metadata.addedByUserId` has no upstream
> face here). The write source is `GroupHelper.updateGroupV2`'s three-way
> removeMembers discrimination (GroupHelper.java:759-770): entries that are
> pending members ride `GroupV2Helper.revokeInvitedMembers`
> (GroupV2Helper.java:320-332, the GroupsV2 delete-pending-member change),
> entries that are full members ride `removeMembers`, entries that are
> requesting members ride `refuseJoinRequestMembers` — the shared array
> means a naive "revoke these invites" call would remove ordinary members
> or refuse join requests, exactly the mis-target family the §4.53
> approval fold exists for on the other half.
>
> **Official desktop surface being aligned (Signal-Desktop,
> PendingInvites.dom.tsx, line anchors from the AGPL tree — behavior
> reference only, zero code reuse):** the conversation-details page shows
> an always-present "Requests & Invites" row for every live group
> (ConversationDetails.dom.tsx:721-730 — `invitesCount` =
> pendingMemberships + pendingApprovalMemberships, **no admin gate on the
> row itself**), pushing a GroupInvites panel with two tabs: Requests (the
> §4.53 approval face) and Invites (this face — `PendingInvites.dom.tsx`
> tab split :92-101, default tab Requests :83). The Invites tab groups
> pending rows by inviter — "Invited by you" with a per-row trash action,
> "Invited by others" with a per-inviter row and a count
> (:401-518) — and gates the trash actions on `areWeAdmin`
> (:446, :494). Revocation is a staged confirm (REVOKE_INVITE :57-61,
> the confirm copy :274-304), fan-out by invitee ids
> (:175-183). The projection differences KT records: (1) no inviter
> metadata upstream → the desktop cannot group by inviter; KT renders one
> flat invitee list (official "invited-by" grouping degraded — the
> invitee set, the count, and the revoke write are all exact); (2) the
> official row appears for everyone while the actions are admin-only —
> the GroupsV2 server adjudicates admin-ness on the write (non-admin
> answers the UNAUTHORIZED face, forwarded verbatim as UPSTREAM_ERROR),
> so the connector does not gate the read either and lets the desktop
> mirror the official visibility.
>
> **Read face (`groups.get` → `pendingMembers`):** the 1.62
> requestingMembers appendix verbatim with the array swapped — the same
> single-group `listGroups` read (one extra engine round-trip the §4.57
> fold itself spends per revoke), projected at read time to bounded
> `{id, uuid?, name?}` entries (the connector-wide first-wins address
> ladder, the uuid only when it differs, the name from the read-time
> contacts cache; bounded to the §4.39 roster cap, malformed entries drop
> individually), **present only when the engine answered a non-empty
> pending set**, absent otherwise; pre-1.64 hosts read byte-identical
> responses; best-effort degradation (engine stopped / refused read → the
> cached row without the key; the §4.8 promise intact). The method stays
> ungated like every §4.8 cache read; the desktop consume batch gates the
> UI on the `group-invite-revoke` capability + this key's presence.
>
> **Write face (`groups.revokeInvites`):** the `groups.approveJoinRequests`
> §4.53 discipline verbatim minus the approve bit — params
> `{accountId, conversationId, members, operationId?}` (members = the
> candidate set, 1-1001 entries of 1-128 characters, the §4.39 roster
> bounds; an empty array is refused, the §4.44 no-delta discipline;
> duplicates collapse; conversationId must resolve to a kind='group' row
> before any engine call — direct answers INVALID_REQUEST, unknown answers
> CONVERSATION_NOT_FOUND). The fold: before anything goes upstream the
> connector reads the engine's own `listGroups {account, groupId}` and
> intersects the candidates with the projected pending identities (number
> and uuid both ride one identity set). Only pending survivors can ever
> ride the upstream `removeMembers` array — every other change key stays
> absent. A fold that empties the candidate set answers
> `INVITE_NO_LONGER_PENDING` (retryable) and **no write is dispatched**; a
> read face that cannot answer fails closed with its own engine error.
> Result semantics (the groups.terminate discipline verbatim): a definite
> success answers `{"status": "sent"}`; an indeterminate upstream outcome
> answers SEND_OUTCOME_UNKNOWN and is never retried. Zero local writes:
> the roster converges through contacts.sync. Error ladder:
> shape/resolution errors `INVALID_REQUEST` / `CONVERSATION_NOT_FOUND`
> (retryable=false, before any engine call); upstream non-admin forwards
> verbatim as `UPSTREAM_ERROR` (retryable=true, the §4.50 family); the
> stale-fold `INVITE_NO_LONGER_PENDING` (retryable=true); unknown outcome
> `SEND_OUTCOME_UNKNOWN` (retryable=false, never retried). The kt-engine
> face has no updateGroup member-family method (the §4.53 engine-debt
> precedent verbatim) and answers `CAPABILITY_UNAVAILABLE`
> (retryable=false) after local validation.
>
> **Capability tag: `group-invite-revoke`** joins `FEATURE_CAPABILITIES` —
> a new tag, not a reuse of `group-join-approve`: the two tags mark two
> protocol actions with separate desktop surfaces (the approval consume
> batch already ships gating on its tag; rebinding its meaning to also
> cover revocation would make an approval-aware desktop's capability check
> silently claim a face its connector may not carry), and the 1.53-appendix
> "one capability for one protocol action's read and write halves"
> discipline applies to this action's halves instead. The desktop consume
> batch gates the invites surface on the tag (a pre-1.64 connector answers
> a clean capability gap instead of METHOD_NOT_ALLOWED). Schema: the
> method enum entry, the `groupsRevokeInvitesParams` def, the allOf
> dispatch branch, the `INVITE_NO_LONGER_PENDING` error-code entry, the
> `groupsGetParams` description widening, and the roster-bounds mirror
> rows in the schema-consistency test land together with the code.
> Fixture: `KT_FAKE_PENDING_GROUPS` (`<groupId>=<addr,addr>;…`) rides the
> listGroups JsonGroup with the `pendingMembers` array; the updateGroup
> route grows the `[fixture-revoke-forbidden]` user-error sentinel and the
> `[fixture-revoke-crash]` `os._exit(37)` sentinel (disjoint from the
> §4.53 crash 36). Tests: the prepare unit face (happy path + duplicate
> collapse; the ladder — empty members, oversized array, per-entry shape,
> unknown conversation, direct addressing, over-bound audit id; the
> engine-mode CAPABILITY_UNAVAILABLE after validation, and
> INVALID_REQUEST precedence on the engine face), the pending-projection
> unit face (ladder id + uuid + read-time name; malformed drop; cap holds;
> absent on None/empty; no inviter metadata ever rides a row), the fold
> unit face (dual-identity capture, malformed entry drops, foreign-shape
> fail-closed, survivor order), and the server integration (handshake
> advertises the method + tag; the live `pendingMembers` key rides a real
> engine answer through the full listGroups → projection ladder; the fold
> keeps only the pending survivor in the recorded upstream payload; the
> stale fold answers INVITE_NO_LONGER_PENDING with zero writes; unknown
> conversation / direct / unknown-params-key / empty-members answer before
> any write; the upstream refusal forwards UPSTREAM_ERROR; the in-flight
> crash answers SEND_OUTCOME_UNKNOWN and is never retried across the
> restart).
### 4.61 Safety-number faces: identities.get, identities.verify (contract revision 1.68, 2026-10-06; §4.57/§4.58/§4.59 already landed in the chain above — the 1.64 invites, 1.65 MRR-projection, and 1.66 forward-multi-attachment batches; §4.60 lands in this same stack — the 1.67 member-label batch's entry sits below)

The safety-number matrix item (the official safety number convergence flow: the details row +
the viewer modal + the verify action) closes its data plane as **one thin managed read plus one
thin managed write over the pinned upstream's native identity faces** — no upstream source
copied, no protocol invention, no engine change. The read face is `listIdentities` (pinned
checkout `v0.14.8` checkout 472784e6: Rust JSON-RPC `client/src/jsonrpc.rs:121-126` — a native
`#[method]` registration taking `account?` and `number?`; the Java command
`src/main/java/.../ListIdentitiesCommand.java:23` implements `JsonRpcLocalCommand`, so the
JSON-RPC projection is native, not a Manager-internal-only ladder). The write face is `trust`
(`jsonrpc.rs:374-381` — `trustAllKnownKeys` XOR `verifiedSafetyNumber`,
`TrustCommand.java:38-60`); the connector carries **only the `verifiedSafetyNumber` form** —
the official desktop's "Mark as verified" semantic, the digits the user just compared
travelling back as the verification confirmation (`TrustCommand.java:37` "only use this option
if you have verified the safety number") — and never the testing-only `trustAllKnownKeys`
mode. The official toggle's clear-verification half (`toggleVerified` → `setVerifiedDefault`,
upstream desktop `ts/models/conversations.preload.ts:3323-3328`) has **no pinned JSON-RPC
projection** (the pinned jsonrpc surface carries no untrust/set-default method) and stays out
of this contract — recorded honestly, not papered over.

- `identities.get` (host method, capability `safety-number`, read lane, non-mutating): params
  `{accountId, conversationId}` — the same addressing pair as the §4.47/§4.56 faces, the way
  the official details row addresses the feature (`!isGroup && !conversation.isMe`,
  upstream desktop `ConversationDetails.dom.tsx:654-660`). Resolution: the §4.47/§4.56 ladder
  verbatim, gated to the 1:1 shape — group/legacyGroup rows fail closed `INVALID_REQUEST`
  before any upstream call (identity keys are a per-contact concept), and so does the §4.42
  self row (empty peer reference). The upstream key is the resolved peer reference verbatim:
  the pinned recipient parameter accepts the phone number / ACI UUID / `PNI:` / `u:` username
  forms (`RecipientIdentifier.java:26-49`), which is exactly the connector peer_key shape
  taxonomy, so the connector passes it through and never re-classifies. The upstream call is
  `listIdentities {account, number}` — narrowed to one recipient, never the account-wide list.
- `identities.get` result `{identity}`: the pinned `JsonIdentity` fields verbatim
  (`ListIdentitiesCommand.java:88-96` — `number?`, `uuid?`, hex `fingerprint`, formatted
  `safetyNumber` (12 spaced 5-digit groups, `Util.java:52-67` — the official desktop's
  `numberBlocks.join(' ')` rendering, `safetyNumber.preload.ts:21,65-68`), base64
  `scannableSafetyNumber?` (the scannable-fingerprint bytes the desktop renders as the QR),
  three-value `trustLevel` (`TrustLevel.java:5-8` — the desktop derives the official
  `isVerified` presentation as `trustLevel === 'TRUSTED_VERIFIED'`), `addedTimestamp`). An
  empty upstream array is the honest `identity: null` — the official desktop's
  SafetyNumberNotReady face ("A safety number will be created with this person after you
  exchange messages with them", `SafetyNumberModal.dom.tsx:35-54`): a state, never an error.
  A record missing its non-nullable core is a protocol defect, not a no-identity answer.
- `identities.verify` (host method, capability `safety-number`, send lane, account-scoped
  mutating — the reaction-class write-lane mutex and delete drain barrier verbatim): params
  `{accountId, conversationId, safetyNumber}` — the safety number string exactly as the
  desktop displayed it. The upstream parses spaced 60-digit groups, 66-char hex fingerprints,
  or base64 scannable payloads (`IdentityVerificationCode.java:20-32`); the connector
  enforces only the 256-char wire bound and never invents a second grammar — a malformed or
  mismatched string is the upstream's definite UserError, folded into the plain
  `UPSTREAM_ERROR` like every account-domain method (no per-failure ladder exists to
  honest-map). Result `{status: "verified"}`: the pinned face answers an empty result object
  on success (the command writes no JSON output, `TrustCommand.handleCommand`), so the
  outcome verb is the honest whole of what the connector knows. An indeterminate outcome
  answers `SEND_OUTCOME_UNKNOWN` and is never retried (the trust write may already have
  landed).
- Zero local writes on both faces: trust level converges through the next `identities.get`
  (the desktop refreshes its read after a verify instead of echoing a local flag — the
  upstream identity store is the whole truth).
- Desktop gating: capability `safety-number` (new tag, one capability for the read and write
  halves — the 1.53 appendix discipline). A desktop without the tag keeps its current
  rendering (zero calls, no details row); contract 1.63 wire consumers are unaffected.

### 4.58 Message-request receive projection: `messageRequestAccepted` on the conversation summaries (contract revision 1.65, 2026-10-06; consume half closed by revision 1.70, 2026-10-07 — see the dated note below)

The message-request data plane closes its read half as a **read-time projection
with the official `isConversationAccepted` factor weights** (upstream
`Signal-Desktop` v8.31.0-alpha.1 `ts/util/isConversationAccepted.preload.ts:21-82`,
AGPL read-only anchor, zero copy): `conversations.list` rows and the single
`conversation.changed` summary carry `messageRequestAccepted` — a bare
always-serialized bool (the muted/pinned/isSelf discipline, both values
meaningful). A desktop consumes the key as authoritative the moment it is
present and keeps its local window derivation only for pre-1.65 connectors
where the key is absent.

Reachability record (2026-10-06, all anchors read from the pinned trees):

- **Path A confirmed for the factor trio on signal-cli**: the pinned
  `v0.14.7-4-g472784e6` `listContacts` row (`JsonContact.java:13-35`) carries
  `isBlocked`, `isHidden`, and `profileSharing` natively — the exact
  official-factor raw material. The connector stamps it through the
  **existing throttled title enrich**: `conversations.list` already spends one
  `listContacts {account, allRecipients: true}` round trip per 60s window
  (`enrich_conversation_faces_throttled`, the renamed
  `enrich_conversation_titles_throttled`); the stamp rides the same answer
  into the new additive nullable `conversations.mrr_factors_json` column
  (schema version 20). Per-row stamps: exactly the flags the engine item
  exposed (`hidden` / `profileSharing` / `blocked`, upstream names minus the
  `is` prefix to match the official factor vocabulary); an item without any
  of the three stamps the honest probed-absent `{}` so the probe never
  re-fires for that row. NULL rows (fresh, never probed) are never
  backfilled — the probe widens the enrich trigger only when every title is
  resolved and a direct row still owes its stamp (a data window without an
  outgoing row: empty windows are already accepted by the official
  empty-private-conversation weight and outgoing-bearing windows by
  `sentMessageCount`, so the probe never wakes for them), so a fully-probed
  account costs zero extra engine traffic.
- **Engine gap, recorded and never invented — the ACCEPT stamp**: the
  official factor's first weight (`messageRequestResponseType === ACCEPT`,
  :33-39) has no honest source on either engine's JSON-RPC receive face. The
  pinned signal-cli projects `syncMessage.messageRequestResponse` only on its
  plain-text path (`ReceiveMessageHandler.java:413-424`); the JSON-RPC wire
  face (`JsonSyncMessage.from`, `JsonSyncMessage.java:41-64`) omits it
  entirely (the wire syncMessage carries sentMessage/blocked/readMessages/
  type only), and the kt-engine receive projection
  (`kt-signal-engine/src/service/mod.rs` `project_content`
  SynchronizeMessage arm) drops it in its `_ =>` fall-through. The §4.56
  write face (`conversations.acceptRequest`/`declineRequest`, upstream
  `sendMessageRequestResponse`) therefore cannot be observed back on today's
  engines — the multi-device echo arrives as a `messageRequestResponse` sync
  the connector's `normalize_receive_fields` files under its syncMessage skip
  arm. The factor collapses to inactive exactly as the official
  `(attr || default)`-style defaults do; engine-side extension (project the
  face on the JSON-RPC receive, or expose a conversation-level accepted
  query) is the recorded follow-up, engine-repo work out of this batch's
  boundary.

> **Dated note (2026-10-07, message-request consume batch, contract revision
> 1.70):** the ACCEPT-stamp engine gap above is closed from the connector
> side of an already-delivering wire. The protocol face
> (`SignalService.proto:865` — the SyncMessage.content oneof member 14;
> `:666-678` — the `MessageRequestResponse` record: `threadAci` ServiceId
> UUID string, `groupId` 32-byte standard-base64 group master key, `type`
> numbered enum UNKNOWN=0/ACCEPT=1/DELETE=2/BLOCK=3/BLOCK_AND_DELETE=4/SPAM=5/
> BLOCK_AND_SPAM=6) reaches this connector whenever its engine projects the
> sync (the official desktop consumes the same face at
> `MessageReceiver.preload.ts:3104` dispatch / `:3274` handler, with
> `GROUPV2_ID_LENGTH = 32`, the UUID shape gate, and `isKnownProtoEnumMember`
> forcing an out-of-enum `type` to UNKNOWN at `:3304-3309`; the conversation
> attr write at `background.preload.ts:3774`; the first factor weight at
> `isConversationAccepted.preload.ts:38`... :33-39). The connector consumes
> it on the receive path as a control receive (the `groupUpdated`
> projection precedent):
>
> - **Normalize + shape gates (engine.rs, fail-closed)**:
>   `normalized_message_request_response` accepts the face only when `type`
>   is a known non-UNKNOWN member (1..=6 — UNKNOWN carries no
>   session-actionable state and drops as skip, the same posture the
>   desktop's UNKNOWN default produces), exactly one thread leg is present
>   (the proto oneof), the ACI leg is the 8-4-4-4-12 hex ServiceId shape,
>   and the group leg standard-base64-decodes to exactly
>   `GROUPV2_ID_LENGTH = 32` bytes. Any hostile or foreign shape falls
>   through to the plain syncMessage skip arm — zero factor material, zero
>   row (宁缺勿错).
> - **Consume (service.rs, `apply_message_request_response`)**: the mirror
>   is intercepted before the generic conversation ladder (the §4.37
>   view-once precedent) and resolves its thread across conversations — the
>   direct leg through the raw ACI, the recorded ACI→peer-key mappings, then
>   the contacts digit-suffix matches; the group leg through the raw group
>   master key (then refused: the request family is 1:1 only, groups carry
>   no request). A thread nothing local resolves drops warn-only. No message
>   row, no system row: the face is state convergence, and the refreshed
>   summary rides `conversation.changed` so every linked device converges.
> - **Factor landing (store.rs)**: the authoritative stamp merges into the
>   row's `mrr_factors_json` as the new `acceptResponseType` (bool: exactly
>   the ACCEPT presence the official factor reads) plus
>   `acceptResponseTypeMember` (the raw enum value) keys — a read-modify-
>   -write so the receive writer and the throttled listContacts enrich
>   writer each own their key family and never clobber each other. The
>   derivation (schema unchanged, wire key unchanged: the same bare
>   always-serialized `messageRequestAccepted` bool) reads the stamped
>   ACCEPT first, ahead of the window weights, exactly the official factor
>   order (:33-39); every other member keeps the row's window-derived
>   answer. No stamp — every pre-1.70 row — reads inactive there and keeps
>   the 1.65 local fallback verbatim (zero regression). The stamped row no
>   longer owes the factor probe (the same `mrr_factors_json IS NOT NULL`
>   marker the enrich stamps set).
> - **Priority, stated once**: authoritative factor present → it decides
>   (ACCEPT ⇒ accepted; any other member ⇒ the window weights, unchanged);
>   factor absent → the 1.65 derivation verbatim. The wire shape never
>   changed, so the desktop batch consumes the new authority with zero
>   desktop-side edits.
> - Tests: the engine gates (well-formed direct ACCEPT/DELETE/BLOCK, the
>   group leg, the seven hostile shapes), the store merge discipline
>   (ACCEPT flips an incoming-only row, DELETE keeps it pending, the
>   enrich re-stamp preserves the accept pair, non-direct refusal, probe
>   silence), the service ladder (mapping resolution, unknown-thread drop,
>   group refusal, no-row discipline, idempotent re-push), and the
>   full-stack round trip over the fake engine
>   (`tests/message_request_response_integration.rs`, standalone file: the
>   ACCEPT ride on `conversation.changed`, the no-row discipline, the
>   DELETE/BLOCK terminal stamps, the four hostile shapes changing nothing,
>   and the unresolvable group mirror inventing nothing).
- **Other collapsed factors, same discipline**: `messageCountBeforeMessageRequests`
  (:60, :75) is official-desktop migration state no engine projects;
  `removalStage` (:33-35) fires only when defined and is never stored here;
  the group factors `addedBy`/`pendingAdminApprovalV2`/group
  `profileSharing` (:45-53, :70-72, :90-103) have no engine read face —
  groups are not contacts and `listGroups` carries none of them. A group row
  therefore derives accepted from its own outgoing window alone.

Derivation (one guarded lookup per row, the 1.27/1.60 post-map loop family —
list page and single-row summary share the same helper so a reloaded page and
an event-driven window cannot disagree):

- `isMe` → the row's `is_self` (always accepted, upstream :25-28).
- empty direct data window → accepted (upstream :63-69
  `isEmptyPrivateConvo`: an empty conversation never shows the request UI;
  the empty-group `isEmptyWhitelistedGroup` twin needs the unseen
  whitelisted bit, so an empty group stays pending — the honest hole).
- `sentMessageCount > 0` → the row has any outgoing data row (upstream
  :58, :75; system rows excluded by the same wall the wire
  `lastMessageDirection` draws).
- `profileSharing` / `isInSystemContacts` → the stamped
  `profileSharing == true` / `hidden == false` bits, direct rows only
  (upstream :90-96).
- everything else after the authoritative stamp (`messageCountBeforeMessageRequests`,
  group factors) → inactive, as recorded above; the stamped ACCEPT (the
  1.70 dated note) reads ahead of this line by design.

Bounds and failure posture: the factor stamp is a stored engine projection —
no message bodies, no raw envelopes, nothing the §1 boundaries exclude; the
stamp's stored keys are three booleans capped by construction; a malformed or
absent stamp degrades to the all-absent default (a hostile stamp can never
fail a summary read); the derivation is a single indexed EXISTS pair plus one
row column, the same per-row cost family as the 1.27 meta lookups. Pre-1.65
hosts read one added always-present key per row (wire addition only, no
renames, no removals); the store migrates 19→20 additively and never
rewrites a row's past. Tests: the unit faces (self row accepted; empty
direct accepted; direct with only-incoming window pending, flipped accepted
by each stamped factor; group with only-outgoing accepted, group
only-incoming pending; stamp degraded default absent; system-only rows
excluded), the enrich faces (trio stamped from one listContacts answer;
item without flags stamps `{}`; probe trigger stays quiet on a fully
probed account), the wire byte faces (pre-1.65 host compatibility is
informational — the key is additive), and the server integration (the key
rides a real store page through the full list → projection ladder).

### 4.59 Multi-attachment send: messages.attachments.send `attachments` array (contract revision 1.66, 2026-10-06)

The official forward face (v8.31 `maybeForwardMessages`): a forwarded multi-
attachment message is **one** upstream message — `enqueueMessageForSend`
carries every attachment of the source message in one `enqueuedMessage`
(attachments array), not one message per attachment. The connector's §4.12
entry point was the only bottleneck: the upstream `send` has always accepted
an array-typed `attachments` parameter (data URIs; the pinned engine
`methods.rs` parses `attachments` as a string array and its
`upload_attachments_for_send` uploads the batch against a per-message ceiling
of 32, fail-closed past it), and `dispatch_send`/`upstream_text_send_params`
already thread `Option<Vec<Value>>` — the wire shape downstream of the
service layer never changed.

`messages.attachments.send` (§4.12) therefore gains an optional **plural
form**, evolving in place on the §4.30/§4.36 precedent (optional params on
the same method, same capability — no new capability tag, the method stays
listed under the same handshake entry):

- Params gain `attachments: [{dataBase64, sizeBytes, filename?, contentType?}]`.
  Each entry is the §4.12 budget family verbatim (padded base64 whose decoded
  size must equal `sizeBytes`, ≤ 104857600 bytes; filename 1–128 bytes with
  the same path-separator/control-character rejection; contentType `type/
  subtype`). 1..=32 entries (`MAX_SEND_ATTACHMENTS`, the upstream per-message
  ceiling mirrored — entries past 32 are rejected INVALID_REQUEST here, not
  as a late engine internal error); the entries' **combined** raw size stays
  within the §4.12 100 MiB message budget, so a plural request still fits one
  host frame and no entry can exceed the size the singular form already
  proves fits.
- **Mutually exclusive with the singular form.** Any singular key
  (`dataBase64`/`sizeBytes`/`filename`/`contentType`) beside the
  `attachments` array — or a singular send missing `dataBase64`/`sizeBytes` —
  is `INVALID_REQUEST` before the pending row exists. A single-element
  `attachments` array is semantically equivalent to the singular form (same
  upstream wire shape, same idempotency chain, same projection): old callers
  and new callers converge on the same row shapes. The schema expresses the
  exclusivity with if/then guards; the host enforces it deterministically.
- **voiceNote and viewOnce stay singular-form faces**: either marker set on
  the plural form is `INVALID_REQUEST`. A voice note is always a single-
  attachment message and official view-once media is a single unadorned
  medium — neither face produces a multi-attachment message, so the plural
  form carries no per-entry voice flag and no view-once semantics (the
  descriptor `isVoiceNote` is false for every plural entry).
- `text` (caption) and `quoteMessageId` are message-level and shared by both
  forms. The pending row carries one metadata-only descriptor per entry, in
  array order (descriptor ids derive from the entry index), so the desktop
  renders every outgoing attachment from the first pending tick and the
  conversation preview falls back to the first filename when the caption is
  empty. Settlement is the §4.12 chain unchanged: one upstream mutating
  `send`, the sent `MessageRecord` on confirmation, `SEND_OUTCOME_UNKNOWN`
  on an indeterminate outcome, no auto-retry. Idempotency: the
  `clientRequestId` short-circuit runs before addressing resolution exactly
  like the singular form.
- Caller migration (the desktop consume batch): the forward path picks the
  plural form when the draft carries ≥ 2 attachments and no voice entry, and
  falls back to the singular per-attachment chain when the connector answers
  a deterministic rejection (an older connector's `deny_unknown_fields`
  params deserialization fails closed on the unknown `attachments` key
  before anything is sent) — never on `SEND_OUTCOME_UNKNOWN`, where a
  fallback send could double-deliver. The backward-compat story is
  byte-identical to every optional-param revision: a pre-1.66 caller sends
  no new key and reads the old shapes unchanged.

### 4.60 Group member-label read face: `groups.get` roster label projection (contract revision 1.67, 2026-10-06; §4.56→§4.60 renumbered on landing — §4.57 landed as the group-invites face, §4.58 landed as the message-request receive projection, §4.59 landed as the multi-attachment send face)

The official member-labels feature (Signal-Desktop v8.31.0-alpha.1:
`ts/components/conversation/conversation-details/GroupMemberLabelEditor.dom.tsx`,
`ConversationDetails.dom.tsx:704-720` list row, `ConversationDetailsMembershipList.dom.tsx:
203-237` roster rows) closes its connector data face. The pinned signal-cli v0.14.8 carries
both halves natively: the write side landed with §4.44 (`groups.update` forwards
`memberLabel` / `memberLabelEmoji`; Java `GroupHelper.updateGroupV2` sets **the linked
account's own** label — `labelEmoji` falls back to the self member's current value when
absent, `GroupV2Helper.setMemberLabels` → `createChangeMemberLabel(getSelfAci(), ...)`; there
is no admin-over-other-member path in the upstream command, and clearing is impossible
because the connector's 1..256-byte bound refuses the empty string that would clear), and
the read side is this batch: `listGroups` member entries carry `labelEmoji` / `label`
(`ListGroupsCommand$JsonGroupMember`, both `@JsonInclude(NON_NULL)`, fed from
`GroupMemberInfoV2.labelEmoji()/labelString()` over the decrypted member).

- Sync capture (`group_extra_json`): each bounded roster entry now stores `labelEmoji` /
  `label` verbatim under the upstream's own server budgets — emoji ≤64 bytes, string ≤512
  bytes (`ts/types/GroupMemberLabels.std.ts` `SERVER_EMOJI_BYTE_LIMIT` /
  `SERVER_STRING_BYTE_LIMIT`); a value past its budget (or an empty/whitespace one) drops
  the key instead of truncating — a clipped label would render a lie. Engines without a
  label face (and every pre-1.67 row) never set the keys; entries keep dropping
  individually on any malformation.
- Read projection (`project_group_roster`): `members[].labelEmoji` / `members[].label` are
  absent-when-none keys — absent for label-less members, for engine-mode rosters, and for
  pre-1.67 rows (byte-identical wire). The read re-checks the same budgets (cheap defense
  in depth). The §4.44 write converges through the next sync; this face projects, it never
  composes.
- Capability: none new — the write half shipped under `group-management` (§4.44) and this
  is a read projection of the same family (the §4.50 appendix / §4.53 appendix discipline).
  The official client's own gate is a remote-config flag
  (`desktop.groupMemberLabels.edit.beta` / `.prod`), a desktop rollout control, not a
  protocol capability; desktops gate the editor on `group-management` plus the roster admin
  mark (the official `getCanAddLabel` admin arm — the group's `accessControl.memberLabel`
  permission is not projected by the pinned upstream, so the knowable arm is the only one
  rendered).
- Schema: `groupsGetParams` description extended; no new method, error code, event, or
  store column (the pair rides the existing §4.39 `group_extra_json` roster). Metrics
  classify as contacts like the rest of `groups.get`.

### 4.62 Send-side text formatting: messages.sendText `textStyle` ranges (contract revision 1.69, 2026-10-06; the receive half landed as §4.64 with 1.72)

The outbound half of the §4.20 rich-body receive face: the caller can style
its own outgoing text. `messages.sendText` gains an optional `textStyle:
[{style, start, length}]` array beside the §4.29 `mentions` face — the
official bodyRanges write face. Upstream anchors, verified against the
pinned distribution: Signal-Android's `TextStyle` vocabulary — BOLD / ITALIC
/ STRIKETHROUGH / MONOSPACE / SPOILER (`NONE` is the internal unstyled
marker, never a valid range value); signal-cli's `SendCommand --text-style`
accepts `start:length:STYLE` triples over UTF-16 code units (the BodyRange
semantics the §4.29 mentions face already rides — the caller indexes its own
body), and the JSON-RPC face carries the camelCase `textStyle` array. The
official desktop composer (format menu + keyboard shortcuts, i18n keys
`FormatMenu--guide--*` / `Keyboard--composer--*`) is the reference producer,
and the §4.20 receive projection already renders every one of these styles —
this revision changes only the send lane.

Validation, all deterministic INVALID_REQUEST before the pending row exists:

- The style vocabulary fails closed: an unknown or `NONE` style rejects the
  whole request. This is deliberately stricter than the §4.20 receive cap
  (drop the entry, keep the message — a hostile payload must not fail a
  peer's message): the sender's own ranges are caller-authored, so a buggy
  or hostile list surfaces at the caller instead of silently rewriting what
  the caller asked to send.
- At most 64 entries (`MAX_SEND_TEXT_STYLES`, the §4.20 receive cap
  mirrored) — past it INVALID_REQUEST, never silent truncation.
- No body-bounds gate: the official `parseTextStyles` validates no range
  against the body and the official desktop clips its own ranges before
  send; the connector neither checks nor clips and passes the offsets
  through verbatim.

Projection and persistence:

- `upstream_text_send_params` (the §4.26 send/retry shared param owner)
  projects each entry verbatim as `{style, start, length}` under the
  upstream `textStyle` key. An absent or empty list leaves the key absent —
  every existing wire shape stays byte-identical (the §4.36 `viewOnce` key
  discipline).
- A styled send persists its ranges on the pending row's rich payload (the
  §4.36 view-once marker precedent): the sender's bubble keeps its own
  formatting across restarts, mirroring the official sender echo. Mentions
  stay unpersisted (the 1.34 shape unchanged).
- Retry (§4.26) rebuilds the send without the ranges (documented deviation,
  the §4.29 mentions precedent): the retried text arrives unstyled upstream,
  and the row's persisted echo remains the sender's record of the original
  attempt.

Engine modes: stock signal-cli carries the `textStyle` param natively. The
kt-engine send face does not parse it yet (engine `methods.rs` send audit:
quote/preview/attachments/mentions/sticker only), so in kt-engine mode a
styled send rides upstream without the ranges — the peer receives the body
unstyled while the sender row keeps its persisted formatting (the recorded
engine-batch debt family of `groups.quit`/`groups.terminate`; the §4.20
receive face is unaffected since 1.25). The connector does not gate or
reject on engine mode: the param is part of the contract face and the debt
is the engine's to retire.

Caller migration (the desktop consume batch): the composer writes
`{style, start, length}` ranges over the draft body and the send carries
them only while the text leaves unchanged — a translation rewrite drops the
ranges (the offsets would not survive a body change), retries ride the bare
deviation above, and the optimistic row merge re-reads the persisted rich
payload so the sender's own bubble echoes the styles with no extra wire.
The composer has no inline rich preview (a plain textarea renders no
ranges); the official inline preview is a documented projection difference.
A pre-1.69 caller sends no new key and reads the old shapes unchanged.

### 4.63 Outbound evidence bit: `hasUserInitiatedOutbound` on the conversation summaries (contract revision 1.71, 2026-10-07; 1.72 has since landed — the §4.62 receive half, its entry sits directly above this section)

The desktop evidence-gate data-plane gap (its official parity matrix §18.4,
2026-10-06 record, read from the desktop repo `signal/default-timer-persist`
branch) closes on the connector side. The official desktop's
`hasUserInitiatedMessages` gate is a **whole-history** truth:
`EXISTS(… isUserInitiatedMessage IS 1)` over the full history index
(`Server.node.ts:2389-2410` definition, `Interface.std.ts:486` stats shape,
`conversations.preload.ts:4621` updateLastMessage trigger / `:3752-3755`
consume, `maybeForwardMessages.preload.ts:192-201` forward trigger) —
independent of any message window or the newest row. The desktop's three
existing read faces cannot reconstruct it:

- `lastMessageDirection` (1.27, §4.22) — a newest-row projection; a
  non-outgoing last row says nothing about the history before it.
- the paged message window's outgoing rows — bounded by the pagination
  window; history outside it is invisible.
- `messageRequestAccepted` (1.65, §4.58) — a multi-factor fold whose true is
  three-ways overloaded (`isSelf`, the empty-direct-window weight, the
  `profileSharing`/`isInSystemContacts` bits, and since 1.70 the
  authoritative ACCEPT stamp all flip it); the underlying `sentMessageCount`
  factor row (`mrr_factors_json`, schema v20) never rides the wire, so the
  desktop cannot decompose it. Using its true as suppression evidence would
  wrongly suppress the reachable main path ("contact-added empty
  conversation" / "incoming-only conversation" — official
  `hasUserInitiatedMessages=false`, the banner should hang); only the fold's
  false is decisive negative evidence, and negative evidence never changes
  the hang decision.

This batch ships exactly the missing bit, named per the desktop record
(the recorded options were `hasUserInitiatedOutbound` or a
`sentMessageCount` passthrough):

- **Wire**: every `conversations.list` summary row and the single
  `conversation.changed` summary carry `hasUserInitiatedOutbound` — a bare
  always-serialized boolean (the muted/pinned/isSelf discipline), true
  exactly when the conversation's whole history contains at least one
  outgoing data row. Semantics align with the official
  `hasUserInitiatedMessages`: the same whole-history EXISTS the official
  stats shape answers, with the §4.58 data-row wall (system rows are not
  conversation content — the same exclusion the wire `lastMessageDirection`
  draws) standing in for the official `isUserInitiatedMessage` filter, the
  closest honest material this chain holds. Pure wire addition: no rename,
  no removal, no other shape change — pre-1.71 hosts read byte-identical
  summaries without the key.
- **Derivation path (store → projection)**: read-time, the same family the
  §4.58 accepted derivation runs — the list post-map loop and the
  single-row summary share one helper (a conversation list and an
  event-driven refresh cannot disagree, the 1.60 discipline), each answered
  by one indexed EXISTS over the `messages` table
  (`direction='outgoing'`). The store's factor rows and every counting
  logic are read, never written: `mrr_factors_json` is not consulted (the
  bit does not ride the §4.58 fold), the schema stays at version 20, and no
  capture path changes.
- **Absence degradation**: a row with no store evidence at all — a fresh
  row, or any history the connector does not hold (link-time import gaps
  included) — projects false. Honest absence, never invented: the desktop
  treats false as "no positive evidence" (its gate hangs), which is the
  safe direction for both readers. A conversation row deleted mid-page
  (retention between the page query and the post-map loop) also answers
  false, never errors — the same tolerance the §4.58 derivation and the
  1.27 meta lookups practice.
- **Cost**: one indexed EXISTS per row over the already-held connection
  guard, the same per-row cost family as the 1.27/§4.58 lookups; no new
  table, no new column, no schema migration.

Tests: the unit faces (whole-history semantics — empty window false,
incoming-only false, outgoing-anywhere true even under a newer incoming
newest row, system-only additions invisible, group rows under the same
rule, unknown-row tolerance, the always-serialized wire key on both
projections, list/single-row agreement) and the full-stack round trip over
the fake engine (standalone file, `tests/outbound_evidence_integration.rs`:
the linked fixture peer starts incoming-only with
`hasUserInitiatedOutbound=false`, a real `messages.sendText` round trip
creates the outgoing row and the bit flips true on the refreshed summary,
a second conversation that never sends stays false, and a fresh never-
touched conversation projects false — the factor-absent degradation).

### 4.64 Receive-side text formatting: the bodyRanges pass-through contract (contract revision 1.72, 2026-10-07)

The receive half of the §4.62 send face: a peer's styled message reaches
every wire face this connector serves. The upstream projection is the
pinned signal-cli `JsonTextStyle` record — `textStyles:
[{style, start, length}]` on the dataMessage (and its
`syncMessage.sentMessage` multi-device mirror), the §4.20 vocabulary
(BOLD / ITALIC / STRIKETHROUGH / MONOSPACE / SPOILER) over UTF-16
code-unit offsets. The receive pipeline has consumed it since 1.25
(§4.20): the per-entry shape gate passes only the five renderable styles,
`NONE` and unknown values drop individually (a hostile payload can never
fail the message), start/length are u32-bounded, the list caps at 64
entries past which extras drop, and an all-default result collapses to no
rich payload at all. This batch records the pass-through contract
explicitly and pins it end to end:

- **Wire**: every read face that projects a row carries `textStyles`
  verbatim on the flattened MessageRecord — `message.upserted`,
  `messages.list`, and `messages.search` (§4.30) share the stored shape,
  so no face can disagree. Absence semantics: a row without styles carries
  no key (the §4.20 all-default collapse) — absence means unstyled, and
  pre-1.25 hosts read byte-identical rows. The key's presence is now a
  guaranteed contract face, not an incidental projection: any wire face
  added later reads the same stored shape.
- **Persistence**: the row's existing additive `messages.rich_json`
  column (schema 8 → 9, §4.20) — no new table, no schema store version
  change (schema 20 unchanged). Rich fields never change after receive
  (the §4.20 edit rule: a body edit keeps the original ranges until a new
  body arrives with fresh ones).
- **Gate placement**: the connector consumes and forwards; it neither
  invents nor validates against the body (no body-bounds gate, the
  official `parseTextStyles` posture both faces share). The sender-side
  strictness (fail-closed INVALID_REQUEST) and the receiver-side leniency
  (drop the entry, keep the message) are deliberate mirrors of the
  official client's two faces — a caller's own ranges are authored, a
  peer's are not.
- **Engine modes**: stock signal-cli carries `textStyles` natively on
  receives. The kt-engine receive projection keeps its recorded "Style arm
  not projected" boundary (engine `docs/jsonrpc-compat.md` §5) until the
  parallel engine batch lands its projection; the connector consume side
  is engine-mode-agnostic — the moment an engine projects the key, every
  face above carries it with no connector edit (the same recorded-gap
  family the 1.70 consume batch practiced).

Tests: the 1.25-era unit faces stand (the five-style pass set, NONE and
unknown drop individually, malformed entries and u32 overflow drop, the
64-entry cap, the all-default collapse, the sentMessage mirror); the
store round trip pins the persisted shape (previews/mentions/textStyles/
viewOnce flatten onto the wire row, plain rows stay key-absent); the
full-stack round trip over the fake engine is new and standalone
(`tests/body_ranges_integration.rs`): one styled receive rides
`message.upserted` with its ranges verbatim, the read faces (`messages.list`,
`messages.search`) project the same arrays, a hostile mix keeps only its
well-formed entries, an all-invalid list collapses to the key-absent
plain shape, and a plain body never grows the key.

### 4.65 Username read face, kt-engine opening: `accounts.getUsername`, `username.lookup` (contract revision 1.74, 2026-10-08)

The §4.55 username read faces stop being honest debt on the kt-engine
face: the engine lands its username read-face batch (engine
`docs/jsonrpc-compat.md` §3 — the KT extension methods `getUsername` /
`usernameLookup`), and this batch opens the connector arms against it.
The JVM/native faces keep their recorded posture — the pinned signal-cli
has no local-account username reader (`JsonAccountResponse` is a private
record inside UpdateAccountCommand fed by the write-back only), and
`username.lookup` keeps its pinned `getUserStatus --username` route
there.

- **accounts.getUsername (kt-engine)**: a read-only engine `getUsername`
  call with `{account}`; the answer projects the engine's
  `{username?, usernameLink?}` storage truth — AccountRecord
  username(33)/usernameLink(35), engine-side storage sync is the single
  truth source (the official proto's "only updated on server
  confirm/delete" anchor; the engine never invents a second one). Both
  keys absent = never set: the §4.55 `AccountUsername` shape carries
  both fields null and the call succeeds — the unset state is the
  answer, not an error. The connector persists nothing locally (the
  JVM write-back mirror stays the display path there); the read is
  always live.
- **username.lookup (kt-engine)**: a read-only engine `usernameLookup`
  call with the same frame the JVM face takes (`{account,
  usernames[1..20]}`, entries 1..64 chars — the frame gate is
  mode-shared, the prepare layer is now mode-agnostic). The upstream
  answer is a top-level array of `{recipient, username?, uuid?,
  isRegistered}` records and the projection is byte-for-byte the §4.55
  ladder — a missing field is a protocol face, not a silently-dropped
  item; per-entry failures arrive pre-degraded (`isRegistered:false`)
  by the engine; the engine record carries no `number` key (that key is
  the pinned `getUserStatus` face's alone). Engine-side server path
  (recorded for the audit trail, owned by the engine): anonymous lookups
  only (`GET v1/accounts/username_hash/{hash}`, `GET
  v1/accounts/username_link/{uuid}` — credentials there are a 400, not
  a shortcut), full-name entries hash via the libsignal usernames
  crate, link entries decrypt (`decrypt_username`) then re-hash — the
  official `resolveUsernameLink` order.
- **Lane/capability**: unchanged — both faces keep their §4.55 lanes
  (`accounts.getUsername` read lane, `username.lookup` read lane) and
  the capability `username` tag; no wire shape change, no schema store
  version change.
- **Mode seam**: the supervisor picks the engine method name —
  `getUsername`/`usernameLookup` vs `getUserStatus` — and nothing else
  diverges; the JVM `accounts.getUsername` debt answer stays
  CAPABILITY_UNAVAILABLE after account resolution.

Tests: the two prepare faces pin the new mode seams (kt-engine frames,
JVM debt after resolution, unknown-account-first); a supervisor-level
engine-mode test routes both method names through a live engine process
(the §4.55 fixture gains `getUsername`/`usernameLookup` arms with a
never-set marker): the set shape projects both fields, the unset shape
projects the null face, the lookup array projects entry-for-entry
without inventing a `number` key, and the dispatch log pins the engine
method names and the resolved signal account. The real-sidecar smoke
(engine_integration `kt_engine_sidecar`) extends to the two engine
method names under the same gate as before.

### 4.66 Username write face, kt-engine opening: `accounts.setUsername` two-hop orchestration (contract revision 1.75, 2026-10-08)

The §4.55 username write face stops being honest debt on the kt-engine
face: the engine lands its username write-face batch (engine
`docs/jsonrpc-compat.md` §3 — the KT extension methods `usernameReserve` /
`usernameConfirm` / `usernameDelete` with the official
proof/link cryptography and the AccountRecord 33/35 storage
persistence), and this batch opens the connector arm against it. The
JVM/native faces keep the pinned `updateAccount` route verbatim.

- **Two-hop orchestration**: the kt-engine set path is
  `usernameReserve` (`{account, username}` — bare nicknames allowed, the
  adopted candidate wins: a bare nickname's discriminator is the
  server's pick, the JVM face's `reserveUsernameFromNickname` twin)
  followed by `usernameConfirm` (`{account, username, usernameHash}` —
  the reserve answer feeds the confirm frame verbatim). Each hop is a
  separate mutating call under the groups face's unknown-outcome
  discipline: an indeterminate hop answers SEND_OUTCOME_UNKNOWN, nothing
  retries, and the chain stops (the reservation itself carries no
  user-visible server effect — the official reserve semantics — so a
  torn two-hop converges through `accounts.getUsername` / the engine's
  storage sync). The delete path is one `usernameDelete` `{account}`
  hop; the host answer is `{}` on both faces (the §4.55 shape).
- **Structured codes (new in the host error enum)**:
  `USERNAME_TAKEN` (reserve 409 / confirm 410) and
  `USERNAME_RESERVATION_GONE` (confirm 409 — the reservation expired or
  lost its race; the caller reserves again) answer retryable=false —
  deterministic refusals; `USERNAME_RATE_LIMITED` (413/429) and
  `USERNAME_SERVER_FAILED` (anything else) answer retryable=true. The
  connector's engine allowlist and the `into_api` mapping grow the four
  literals in lockstep (the totality test covers them by construction;
  the retryability faces have their own pins).
- **Honest partial success**: the engine confirms server-side first and
  persists the storage record second — a storage write failure after the
  server's 200 surfaces as the STORAGE_* family (the username is live
  server-side; `accounts.getUsername` / multi-device storage sync
  converge). The connector adds no second truth and no reconciliation
  loop.
- **Boundaries (unchanged from the §4.55 recording)**: no case-change
  shortcut — every set rides the full two-hop (the server is the hash
  truth; the desktop's local case-change rewrite has no connector-side
  mirror); no usernameChangeSyncMessage — multi-device convergence is the
  engine's storage sync, not a second signal; nothing persists
  connector-side (the write-back projection stays the mirror source).
- **Lane/capability**: unchanged — send lane, account-scoped mutating,
  capability `username`; no wire shape change, no schema store version
  change.

Tests: the prepare face pins the kt-engine frames (the same set frame the
JVM face builds; the bare-account delete frame; validation outranks the
mode seam); a supervisor-level engine-mode test rides the §4.55 fixture's
new `usernameReserve`/`usernameConfirm`/`usernameDelete` arms — the
two-hop happy path with the adopted candidate, the bare-nickname pick,
the delete hop, the three structured refusals surfacing as engine
UpstreamCode literals, the torn two-hop (reserve succeeds, confirm
refuses RESERVATION_GONE), the in-flight crash answering
SEND_OUTCOME_UNKNOWN with exactly one reserve dispatch and no confirm
hop (never retried; the explicit restart lands a fresh engine and the
next set works), and the dispatch log pinning the hop order/payloads
with the raw account id never riding the frame.

### 4.67 Stories faces: `messages.sendStory`, the `story.received` event, the `stories.setDisabled`/`stories.getDisabled` toggle (contract revision 1.76, 2026-10-08)

The connector's story faces open with the engine's stories batch (engine
`docs/jsonrpc-compat.md` §3 send row and §5 Stories 发送批 — the KT
extension `sendStory` with the official text-story construction, plus
the receive side's envelope-level `storyMessage` /
`dataMessage.storyReply` projection). Stories never touch the
conversation stream in either direction: the send is rowless, the
receive forwards as one transient event, and the connector keeps no
story timeline — the desktop owns it.

- **`messages.sendStory` (kt-engine face only)**: a rowless send — no
  `clientRequestId`, no pending row, no settlement: prepare under the
  service lock, one account-scoped mutating call, and the synchronous
  `{timestamp, results}` projection is the whole answer (the story's
  own timestamp is its upstream identity; a re-send is a new story). An
  indeterminate outcome answers SEND_OUTCOME_UNKNOWN and nothing
  retries (the §5 send discipline, rowless variant — there is no
  pending row to settle). `recipients` is the caller-computed expansion
  (1..1001 entries): the desktop owns v1 list storage — the official
  desktop expands MY_STORY from its own connections minus exclusions
  (`ts/util/sendStoryMessage.preload.ts:87-116`) and custom lists from
  its own member tables — so the connector neither re-expands nor
  filters. `distributionListId` mirrors the official
  `isStoryDistributionId` face fail-closed: absent means the My-story
  zero UUID, any other value must be a strict UUID and passes through
  lowercased (`normalizeStoryDistributionId`); anything else is the
  officially unreachable shape and answers INVALID_REQUEST before any
  frame (the engine passes the id through without re-validating the
  shape). `textStyle` is the official TextDisplayStyle vocabulary
  (DEFAULT/REGULAR/BOLD/SERIF/SCRIPT/CONDENSED — DEFAULT means the key
  is omitted from the frame), `bodyRanges` ride the §4.62 face verbatim
  (≤64 entries), `previewImage` is a data URI within the avatar
  budget, and `allowsReplies` is always explicit (default false).
  Parameter failures surface twice: connector-side INVALID_REQUEST
  before the frame, the engine's `-32603` beyond it — no new error
  codes (per-recipient upstream failures fold into one upstream error,
  the engine's discipline). The JVM/native faces answer
  CAPABILITY_UNAVAILABLE (retryable=false — the pinned signal-cli has
  no story face); validation outranks the mode seam.
- **The receive face**: a story addresses no conversation, so the
  envelope-level `storyMessage` projection routes with the
  call-signaling posture — control-plane material, no row, no timeline
  state. The bounded projection (the 3 MiB coarse material bound, the
  call face's family) forwards verbatim as the transient
  `story.received` event; an oversized or unusable face drops the whole
  envelope (宁缺勿错). Delivery is at-least-once and the event is never
  persisted: the desktop owns the story timeline and its retention (the
  official 24h expiry is client-side) and dedupes by sender + timestamp
  (the story's protocol identity). The `isStory` bit alone never routes
  — the storyMessage face does; the sender's own linked-device sync
  mirror is not this face (the `{timestamp, results}` answer is the
  sender's local record).
- **Story replies**: `dataMessage.storyReply` (`{authorAci,
  sentTimestamp}` — both required) rides a regular conversation row's
  rich payload (the §4.64 receive-formatting family, store schema 20
  unchanged): a malformed reply drops the key, never the row. The same
  stories-disabled gate faces it in both directions — a peer's reply
  and the linked-device sync mirror alike drop before any conversation
  or row exists; re-enabling stories re-receives nothing (the upstream
  already considers it delivered).
- **The toggle faces**: `stories.setDisabled`/`stories.getDisabled`
  are the connector-local mirror of the official `getStoriesBlocked`
  receive gate (itemStorage `hasStoriesDisabled`, default false —
  stories enabled). Pure local store faces under the existing meta KV
  (no migration, store schema 20 unchanged): setDisabled is an
  account-state write with no engine frame, so it rides the send lane
  behind the same-account mutex and delete barrier (the
  `contacts.setLocalAlias` §4.9 precedent) and answers the stored truth
  read back, not an echo of the request; getDisabled is a bounded
  read. Every engine mode answers both.
- **Lane/capability**: `messages.sendStory` and `stories.setDisabled`
  are send lane, account-scoped mutating; `stories.getDisabled` is
  read. The capability set grows `stories`. The schema event enum grows
  `story.received` (data requires `accountId` + `story`; the optional
  sender/senderAci/senderName/timestamp keys ride the envelope) and the
  request surface grows the three params defs with the bounds pinned to
  the constants; no schema store version change.

Tests: the engine normalization pins the story faces
(`story_envelopes_route_verbatim_and_oversized_skip` — the verbatim
projection plus the oversized-envelope skip;
`story_reply_parses_onto_rich_and_malformed_drops_key`); the service
prepare face pins the gate order and the frame shapes
(`send_story_preparation_gates_then_frames` — validation outranks the
mode seam, the kt-engine lean/full frames, the JVM/native refusal;
`stories_toggle_preparation_pins_account`) and the ingest gate
(`story_ingest_gate_emits_or_drops_by_the_toggle` — the transient
StoryReceived emission vs the silent drop, and the reply row surviving
with its storyReply key intact). The supervisor engine-mode test rides
the fixture's new `sendStory` arm
(`story_faces_route_engine_send_and_local_toggle_in_engine_mode`): the
toggle round-trip from the official default, the lean and full-option
frames carrying the resolved signal account and never the store account
id, the crash sentinel answering SEND_OUTCOME_UNKNOWN exactly once with
no retry, the explicit restart landing a fresh engine, and the dispatch
log pinning all four story dispatches. The schema consistency pairs
grow the four story bounds (message↔MAX_TEXT_BYTES,
distributionListId↔MAX_STORY_DISTRIBUTION_ID_CHARS,
previewImage↔MAX_AVATAR_DATA_URI_CHARS, recipients
items↔MAX_OPAQUE_ID_BYTES; the recipients maxItems cap stays
inventory-side like the sibling roster faces — the array is the
caller-computed expansion forwarded verbatim and the prepare ladder
owns the cap). The real-sidecar smoke grows the `sendStory` arm (the
fake's `{timestamp, results: []}` shape asserted).

### 4.68 Official-desktop snapshot import: `backups.importSnapshot`, `backups.importSnapshotStatus` (contract revision 1.80, 2026-10-08)

The counterpart of §4.46 (the engine *produces* Android-restorable snapshots;
this face makes the engine *consume* official Signal Desktop snapshots). The
official desktop writes local backups under
`SignalBackups/signal-backup-<yyyy-MM-dd-HH-mm-ss>/{metadata, main, files/}`;
the desktop picks the directory (its own OS folder dialog), the connector
forwards the path to the engine (the same-machine path handoff the §4.46
artifact runs in reverse), and the engine answers with the §4.38 NDJSON the
existing import pass already consumes. Anchors (AGPL read-only, zero copy):
`ts/services/backups/index.preload.ts:572-595` (`importLocalBackup`) and
`ts/services/backups/util/localBackup.node.ts` (`verifyLocalBackupMetadata`) —
strict metadata verification (version 1, 12-byte IV, 16-byte encryptedId,
CTR-decrypted backupId compared byte-for-byte) then the official
`FramesReader` decrypt of `main` (HMAC tail verified end to end).

- **`backups.importSnapshot`** `{accountId, snapshotDir}` →
  `{status: "started"}`. `snapshotDir` is the caller-chosen absolute
  directory path (1..4096 bytes; a relative path answers INVALID_REQUEST
  before any engine traffic — the connector never dereferences it, the
  engine is the only reader). The account resolution and engine-mode gate
  ride the §4.38 faces: JVM/native answers CAPABILITY_UNAVAILABLE
  (retryable=false, capability `history-import` — no new capability bit).
  The engine call is Mutating, so an indeterminate outcome answers
  SEND_OUTCOME_UNKNOWN and never retries (the engine's own per-account
  import gate makes a duplicate start fail-closed with
  BACKUP_IMPORT_ALREADY_RUNNING). Engine structured codes pass through
  verbatim (`-1` + message): BACKUP_SNAPSHOT_MISSING,
  BACKUP_SNAPSHOT_METADATA_INVALID, BACKUP_SNAPSHOT_KEY_MISMATCH (all
  retryable=false), BACKUP_SNAPSHOT_IO_FAILED (retryable=true),
  BACKUP_IMPORT_ALREADY_RUNNING (retryable=false). Param-face failures
  (unknown account, missing/empty/relative/too-long snapshotDir) answer
  INVALID_REQUEST `-32603`-class per the host boundary.
- **`backups.importSnapshotStatus`** `{accountId}` → the engine's status
  projection verbatim minus the raw account echo (`accountId` rides in its
  place, the §4.46 precedent): `{state: idle|running|completed|failed,
  ndjsonPresent, startedAtMs?, finishedAtMs?, lines?, frames?, error?}`.
  `lines` counts projected NDJSON rows (recipient+chat+message), `frames`
  counts archive frames including the BackupInfo meta-frame, absent keys
  are omitted rather than zero-filled, and `ndjsonPresent` is the disk
  truth (an engine restart resets the engine-side state to idle while the
  produced NDJSON stays). Read lane; an engine that is down or an
  indeterminate read degrades to the disk-truth idle projection
  (`{state: "idle", ndjsonPresent: <the archive's real disk presence>}`)
  rather than inventing a state (the §4.46 disk-truth posture).
- **The consumption watcher**: on a definite `started` the connector spawns
  a per-account watcher that polls the engine status (~3s, 30-minute
  ceiling) and on a terminal state drives the §4.38 import pass directly —
  `completed` constructs the import job immediately (bypassing the
  link-time probe's completed/attempts gates on purpose: this is an
  explicit user request, not a link-time sweep), reusing the group's
  single-flight import slot; `failed` (or the ceiling/expired-idle escape)
  lands an honest failed ledger row with `error_class = "engine"` so
  `history.importStatus` renders the failure and `history.importRetry`
  answers NO_ARCHIVE (the archive never landed) — the desktop re-invokes
  `backups.importSnapshot` to try again, exactly the official Retry
  semantics. The store ledger row, the masked status face, and the
  retention convergence are the §4.38 faces unchanged.
- **Lane/capability**: `backups.importSnapshot` is send lane,
  account-scoped mutating; `backups.importSnapshotStatus` is read. The
  capability set does not grow: the face rides `history-import` (kt-engine
  only) because its entire output is the history-import archive and its
  ledger. The schema request surface grows the two method enum entries,
  the two dispatch literals, and the two params defs; no schema store
  version change.

Tests: the supervisor engine-mode test rides the fixture's new
`backup.importSnapshot`/`backup.importSnapshotStatus` arms
(`snapshot_import_happy_chain_lands_store_rows` — started → watcher →
engine completed → the NDJSON import pass writes real message rows under
the §4.38 dedupe keys; `snapshot_import_structured_codes_pass_through` —
the five engine codes ride the wire verbatim with the mapped retryability;
`snapshot_import_param_gates_and_mode_seam` — relative/empty/oversized
snapshotDir fail closed before any engine traffic and JVM/native answers
CAPABILITY_UNAVAILABLE; `snapshot_import_engine_failure_lands_honest_row`
— the engine's failed status lands a failed ledger row whose retry
answers NO_ARCHIVE). The schema consistency pair grows the snapshotDir
bound (1..4096); the account id rides the shared opaqueId `$ref`, whose
bound is already paired.

### 4.69 Cloud-backup restore passthrough: `backups.restoreFromCloud`, `backups.cloudStatus` (contract revision 1.83, 2026-10-08)

The engine-side cloud restore (engine `docs/cloud-backup-contract.md` §2,
KT extension methods, worktree `feat/cloud-restore` 9090ec5) lands its
connector pipe: two faces, pure passthrough — the connector writes no local
state, runs no chain stage, and owns no consumption in this batch. The
engine's chain (backup credential fetch with the 404 → `PUT v1/archives/
backupid` bootstrap self-heal → zkgroup redemption/presentation →
unauthenticated chat gRPC backup location → CDN download → AEP decrypt →
NDJSON at `history-import-cloud.ndjson`) runs entirely in the engine's
background task; the faces only start it and observe it.

- **`backups.restoreFromCloud`** `{accountId}` → `{status: "started"}`.
  Send lane, account-scoped mutating — the `backups.export` class verbatim
  (the engine's background restore is account-mutating engine state): an
  indeterminate outcome answers SEND_OUTCOME_UNKNOWN and never retries; the
  terminal state is observed through the status face (the engine keeps the
  failed phase until the next start overwrites it). The seven structured
  engine codes pass through verbatim — the engine's
  `CloudRestoreError::rpc_code` table is the single mapping source, shared
  with the failed-phase `errorCode`, so the two ends cannot drift:
  BACKUP_NOT_PROVISIONED (never provisioned after the bootstrap self-heal),
  BACKUP_AUTH_FAILED, BACKUP_RUNNING (the engine's own atomic re-entry gate
  — poll the status face instead of re-starting) answer retryable=false;
  BACKUP_TRANSPORT_FAILED, BACKUP_DOWNLOAD_FAILED answer retryable=true;
  BACKUP_DECRYPT_FAILED, BACKUP_EXPORT_FAILED answer retryable=false. The
  four message-carrying families carry `error.data {detail}` verbatim — the
  detail is the promised contract face, so unlike the unpromised engine data
  other codes may carry it passes instead of being dropped (the 1.50
  PROFILE_UPLOAD_FAILED pass-through discipline at a fixed shape); the three
  state families stay content-free. A non-started engine answer fails
  UPSTREAM_PROTOCOL_ERROR.
- **`backups.cloudStatus`** `{accountId}` → the engine's progress snapshot
  mirrored: `{present: false}` when no snapshot exists (never started, or
  succeeded — a success leaves no trace, its NDJSON rides the history-import
  chain —, or the engine restarted), else `{present: true, phase:
  "downloading"|"failed", bytesRead, bytesTotal?, errorCode?}`. Read lane,
  non-mutating (the `transfer.downloadStatus`/`backup.exportStatus` read
  discipline). `bytesRead` counts CDN ciphertext bytes; `bytesTotal` is
  absent whenever the engine could not learn a content-length — honest
  uncertainty, never a zero-filled stand-in; the failed phase is a terminal
  snapshot that persists until the next start overwrites it, carrying the
  same structured `errorCode` table as the start face's error ladder. An
  engine that is down (or an indeterminate read) answers `{present: false}`:
  the snapshot is engine-process memory, so "not observable" is exactly the
  post-restart truth, not an invented state. A malformed present-snapshot
  face (wrong phase enum, untyped bytes, junk shape) fails
  UPSTREAM_PROTOCOL_ERROR instead of degrading to `{present: false}` — a
  read that delivered junk knows nothing, and claiming absence would be the
  fake-zero lie in state form. The answer is rebuilt key-for-key through the
  projection's shape gate, so no unplanned key rides. Deliberately no push
  event (the §4.68 polling precedent).
- **Lane/capability**: `backups.restoreFromCloud` is send lane,
  account-scoped mutating; `backups.cloudStatus` is read. The capability set
  does not grow: both faces ride `history-import` (kt-engine only — the §4.68
  mode seam answers CAPABILITY_UNAVAILABLE) because the restore's entire
  output is the history-import NDJSON chain. The schema grows the two method
  enum entries, the two dispatch literals, the two params defs, and the
  seven error codes; no schema store version change.
- **Deliberately out of this batch** (recorded, not worked around): the
  produced `history-import-cloud.ndjson` joining the §4.38 import pass (the
  §4.68 watcher pattern, on the engine contract's follow-up batch), and the
  desktop settings-page backup entry (the engine contract's third follow-up).
  Until the consumption batch lands, a successful restore's archive sits
  engine-side unconsumed — the faces never claim otherwise.
- **The consumption batch lands** (contract revision 1.84, 2026-10-08; the
  engine NDJSON truth re-read, not assumed: engine
  `src/service/cloud_backup.rs` writes the product as
  `<engine data-dir>/accounts/<registry dir>/history-import-cloud.ndjson` —
  the transfer archive's own directory under a distinct file name — with a
  single `fs::write` that runs strictly after `restore_from_cloud` returns
  `Ok`, and the spawn wrapper clears the progress snapshot only after that
  write returns; so file presence is post-success by construction, a failed
  write surfaces the export-failure phase, and a crash mid-write can leave
  only the partial file the §4.38 tolerant parse already survives):
  `backups.restoreFromCloud` now spawns the consumption watcher on a
  definite `started` and on SEND_OUTCOME_UNKNOWN (which may actually have
  started — the §4.68 posture), closing restore → 落消息.
  - **Completion face — the artifact's freshness, not the snapshot**: the
    engine's success leaves no trace (`{present: false}` = never started /
    succeeded / restarted, the §4.69 contract), so `cloudStatus` cannot
    certify success. The watcher captures the cloud archive's `(mtime, len)`
    face before its first sleep — the restore chain cannot have landed its
    artifact in that window, so the captured face is exactly the stale world
    — and treats a changed face as the engine's fresh write: the definite
    success signal. A stale archive from a previous restore (identical face)
    is therefore never imported by a new watcher, and a re-restore's rewrite
    (a fresh mtime) re-arms the import — its rows dedupe onto the same
    identities with the duplicates surfacing in the honest `skippedMessages`
    counter (the importer's own §4.38 unit pin). An empty file is not a
    produced archive (the §4.38 `archive_ready` discipline).
  - **Failure face**: `cloudStatus` rides the loop for `phase: "failed"` —
    the engine's terminal snapshot — landing the honest engine-class failed
    ledger row (`land_import_engine_failure`, the §4.68 helper unchanged) so
    `history.importStatus` renders the failure and `history.importRetry`
    answers NO_ARCHIVE: retry is re-invoking `backups.restoreFromCloud` (the
    official Retry semantics; the retry face's `on_disk` re-arm deliberately
    still probes only the transfer archive — a cloud-failed row must not
    re-arm onto a consumption path nothing watches). The artifact-face
    check precedes the status check each tick: the engine writes the file
    strictly before an export failure maps onto the failed phase, so a
    landed artifact is consumed before a late failure observation could
    clobber a completed ledger row.
  - **Bounds and escapes**: the §4.68 cadence (~3 s polls) under a
    60-minute ceiling (the watched chain includes the whole CDN download).
    The ceiling escape makes one last-chance disk check (a slow restore
    whose artifact landed between ticks still consumes) before landing the
    failed row, and never lands one when a fresh face was seen but the
    group slot stayed busy (a live pass holds the ledger — the §4.68
    completed-seen posture). A deleted account exits quietly; a missing
    registry entry exits quietly (no row was ever claimed, no face promised
    an import).
  - **Status face — reuse,裁定 recorded**: the §4.38 `history.importStatus`
    composition is reused as-is, zero new keys and zero face edits: the
    watcher constructs the same `HistoryImportJob` (the archive path being
    the only difference) through `claim_and_run_history_import`, bypassing
    the link-time probe's completed/attempts gates on purpose (an explicit
    user request — the §4.68 precedent), so the ledger row, the masked
    status face, the 1.53 `processing` holder attribution, and the
    retention convergence are the §4.38/§4.68 faces unchanged. A new cloud
    status key was rejected: the same import pass consumes the artifact, so
    a second key would fork one store truth into two faces the desktop must
    join. During the restore itself (before the import claims) the face
    keeps answering its link-time semantics — the same accepted window
    §4.68 has between the engine task and the import claim; the desktop's
    restore-phase truth is `backups.cloudStatus`.
  - **Boundary discipline (the 838 记档口径 continued)**: neither face
    claims landed rows before they land — `cloudStatus` is untouched (the
    engine's restore truth only) and `history.importStatus` moves only when
    the import pass actually writes; the media tier stays a second-phase
    item and cross-account restore stays out of the face (the engine
    contract's own boundaries). Recorded recovery hole: a connector restart
    mid-restore loses the watcher and the archive sits unconsumed until the
    next explicit restore — the §4.38 link-time sweep stays
    transfer-archive-scoped on purpose (one per-account ledger row cannot
    mark which artifact a completed row consumed; a cloud-aware sweep would
    either re-import under a completed row or need a store change).

Tests: the fixture gains `backups.restoreFromCloud`/`backups.cloudStatus`
arms mirroring the negotiated wire face only — marker files steer the
seven-family ladder and the three status states
(`tests/backup_cloud_restore.rs`: the three-state mirror with the absent
`bytesTotal` staying an absent key; the started acceptance plus the
seven-family ladder verbatim with the contract-fixed retryability and the
detail pass-through; the mode/account/params gates). The unit chain pins the
whitelist sets (`methods.rs`), the seven-code mapping totality with the
content-free/detail split (`service.rs`), and the cloudStatus shape gate
(`supervisor.rs`). The integration roundtrip
(`server_integration.rs::cloud_restore_faces_round_trip_in_kt_engine_mode`)
drives the host IPC end to end in kt-engine mode: the handshake advertises
both faces with no new feature tag, the started acceptance, four ladder
families plus the re-entry gate across the wire, and the three status
states. These pins cover the connector's routing and error fidelity, never
real engine restore behavior — the real-device restore chain is the engine
batch's own acceptance, not this batch's.

Tests (contract revision 1.84): the fixture's `backups.restoreFromCloud`
happy path now runs the background chain (a short delay, then either the
NDJSON product write plus the success-clear, a stall marker holding the
phase, or a failed-phase flip). `tests/backup_cloud_restore.rs` grows four
consumption pins: the happy chain (started → watcher → fresh artifact →
the §4.38 pass lands imported=2 with the transfer artifact never present —
the counters prove the cloud file was the source — and `cloudStatus` riding
cleared beside a completed ledger row), the failed phase (an honest
engine-class failed row, `history.importStatus` rendering it,
`history.importRetry` answering NO_ARCHIVE), the stale-archive guard (a
planted previous archive plus a stalled downloading phase ticks the watcher
twice with the ledger untouched), and the re-restore (the rewrite's fresh
face re-arms the import; the second run completes with the deduped
counters — imported 0, the two duplicates surfacing in `skippedMessages`,
the importer's own §4.38 counter semantics). The unit chain pins the
freshness face itself (absent → `None`, stable while untouched, changed on
rewrite).

### 4.70 Story view receipts: `stories.markViewed` + the `story.viewed` receive projection (contract revision 1.85, 2026-10-08)

The stories watch receipt lands both legs: the send leg lets the desktop
acknowledge a story it rendered as viewed; the receive leg projects the
account's own stories' view receipts as a distinct event instead of
folding them into the §4.29 message fold. The engine's `sendViewedReceipt`
is `{account, recipient (a single author), timestamps[]}` → `{}` — an
empty, missing, or non-numeric timestamps array fails upstream — and the
receive-side receiptMessage projection carries no story bit on the wire
(`{timestamp[], when, isDelivery, isRead, isViewed}`, compat §3):
story-ness exists only on the local row face, which is where the split
lives.

- **`stories.markViewed`** `{accountId, storyAuthor, storySentAt}` →
  `{status: "sent"|"unknown"}`. Send lane, account-scoped mutating. The
  receipt class is the `messages.markViewed` one verbatim: best-effort,
  any upstream failure (a definite error, transport trouble, or an
  unknown outcome) degrades the answer and nothing is ever retried; a
  non-running engine stays a structural error (the send_receipt
  discipline). The §4.29 row selection is replaced by the story anchor:
  exactly one upstream call `{account (the resolved signal account),
  recipient (the trimmed storyAuthor), timestamps: [storySentAt]}` — a
  story receipt names exactly one story, so there is no author fan-out
  and the 100-timestamp chunk framing never engages. `storySentAt: 0`
  answers INVALID_REQUEST (there is no meaningful "mark nothing viewed");
  the author rides the opaque-id bound (MAX_OPAQUE_ID_BYTES). The raw
  store account id never rides the frame.
- **Mode/capability**: mode-independent — the upstream face is the pinned
  upstream's own method, available in every engine mode, so no capability
  seam exists to answer. The capability set does not grow: the `stories`
  tag extends in place (the 1.82 call-signaling precedent).
- **Receive leg (`story.viewed`)**: a VIEWED receipt is intercepted in the
  control-receive ingest before the conversation ladder. With a
  resolvable source: an existing direct conversation first runs the §4.29
  `upgrade_outgoing_receipts` fold (message viewed receipts keep the
  one statusChanged-per-real-transition discipline on the read tier —
  revision 1.94 §4.77 extends that fold to also first-stamp the hit rows'
  `viewedAt`, so an already-read row emits a stamp-only event instead of
  silence), then the
  timestamps whose outgoing rows exist — decided by the hit rows, not the
  moved rows, so a replayed viewed receipt (its row already read and
  already viewed-stamped) leaks
  nothing onto the story face — are consumed, and the leftovers ride
  exactly one transient event `story.viewed`
  `{accountId, sentTimestamps, viewedAt, viewer?, viewerAci?, viewerName?}`
  with the absent-optional-keys discipline (the story.received emission
  shape). A viewer with no direct conversation forwards the whole receipt
  as story candidates — exactly where the pre-1.85 pipeline evaporated
  the envelope on the conversation ladder. A source-less envelope drops
  whole (宁缺勿错: neither a conversation to fold against nor a viewer
  the desktop could record). The stories-disabled gate does not apply: it
  gates incoming story material, not receipts about the account's own
  stories.
- **Boundary discipline**: the event is rowless and transient — no row is
  written, no story state is kept, no self-heal exists (the story.received
  discipline); the split is the connector declaring candidacy, and the
  desktop's own timeline join (sentAt × sender key against its story
  store) stays the authoritative anchor. Second-phase items recorded
  deliberately: a `storyViewReceiptsEnabled` account preference (the
  engine AccountRecord written-bit family) is not in this batch, and the
  desktop wiring (the `signalStoryApplyView` / `signalStoryViewsOf`
  consumption) is its own batch.

Tests: the preparation gates and the frame shape (`service.rs` — empty /
blank / oversized author, zero sentAt, unknown account, then the resolved
signal account with the trimmed author and exactly one timestamp); the
receive split (`service.rs` — the mixed receipt folds its message half
and rides its story half, the replayed viewed receipt stays silent, the
conversation-less viewer forwards whole, a READ receipt never grows a
story leg, the stories-disabled toggle does not apply, the source-less
envelope drops, and nothing persists); and the engine-mode roundtrip
(`supervisor.rs` — `sent`, the in-flight crash sentinel degrading to
`unknown` without a retry, the explicit restart and the next receipt
sending again, and the dispatch log pinning the frame shapes with the
store account id never riding).

### 4.71 Story-anchored attachment bytes: `stories.downloadAttachment` + `stories.openAttachment` (contract revision 1.86, 2026-10-08)

The byte half of the story surface: the spike's §3-connector finding ② —
`attachments.download` is row-anchored
(conversationId + messageId + attachmentIndex, the connector resolving the
row locally to its stored pointer face) and a story has no row to resolve —
closes with a caller-supplied pointer face instead of a story-anchored row
store. The connector keeps no story state (the §4.67 discipline: the
desktop owns the story timeline, the connector owns the pipe), so the
desktop passes back what `story.received` delivered and the connector
re-runs the byte machine it already runs for rows.

- **`stories.downloadAttachment`** `{accountId,
  attachment: {id, size, locator}}` → `{sizeBytes: N}`. Send lane,
  account-scoped mutating (it lands a file in the governed media
  directory; the reaction-class write-lane mutex and delete drain barrier
  serialize it — the 1.57/1.61 class verbatim). The locator is exactly the
  face the engine's `project_attachment_pointer` projection shipped on the
  story event: `{key, cdnKey, cdnNumber, uploadTimestamp?, digest?,
  plaintextHash?}`; `id` is the attachment id that projection carried and
  becomes the landing file name (the sanitizeId-parity gate: 1..=128
  bytes, no path separators, never `.`/`..`); `size` is the declared
  plaintext size — mandatory, because it is the decrypt-trim input, and a
  pointer without it is not downloadable (a trim against a guessed size
  would be the guess). Rendering fields (contentType, filename, width,
  height, caption, isVoiceNote) never ride the params —
  deny_unknown_fields turns the whole projection passed verbatim into a
  params error rather than a silent strip.
- **The shape gate (fail-closed, three layers, all ahead of any network
  traffic)**: the params serde gate (types; required key/cdnKey/cdnNumber;
  unknown keys); the prepare ladder (the §4.54 locator-ingest caps
  mirrored onto the caller-supplied strings — key/cdnKey/digest ≤ 512
  characters, plaintextHash ≤ 128 characters, uploadTimestamp ≥ 1 when
  present — then the id sanitize gate, then the 1..=104857600 size
  bound); and the chain's `TransitFace::build` crypto-shape decode (the
  key base64-decodes to exactly the 64-byte AES+HMAC set, cdnNumber
  present, at least one integrity face — digest or plaintextHash, both
  verify when both ride). Every layer answers INVALID_REQUEST
  retryable=false with a content-free message; a hole is never a guessed
  download, no pointer field is ever invented, and pointer material never
  enters an error message or a log line.
- **The chain after the gates is the §4.38 step-3 machine verbatim** —
  the same `run_attachment_download_chain` history.attachmentDownload and
  attachments.download run: cdnNumber → cdn base map with the CDN-0
  fallback, the encodeURIComponent-parity cdnKey path, streamed GET
  (connect 10 s / total 120 s / no redirects) under hard body bounds, the
  trailing-MAC HMAC-SHA256 gate then the whole-body SHA-256 digest gate,
  AES-256-CBC decrypt (IV-prefixed body, PKCS7), the official log-pad
  trim to the declared size, the plaintextHash face, and the atomic
  landing where `stories.openAttachment` resolves the file. Download,
  integrity, and decryption failures answer UPSTREAM_ERROR
  retryable=false — terminal, never auto-retried (the AGENTS.md
  unknown-outcome red line; a retry is the caller's fresh call); local
  filesystem failures keep the retryable UPSTREAM_ERROR face.
  Already-present files answer the measured size without touching the
  CDN (idempotent dedup — a repeated viewer open re-answers, the CDN is
  never re-asked).
- **`stories.openAttachment`** `{accountId, attachmentId}` →
  `{mediaHandle, sizeBytes, chunkBytes}`. Read lane, non-mutating, the
  messages.attachments.open machine without the row addressing: the same
  capability gate, the same id/sanitize gates, the same governed
  directory probe, the same process-wide handle table (per-account
  budget, TTL, sequential chunks), and the same not-available answer —
  UPSTREAM_ERROR retryable=true, not-downloaded and governor-deleted
  deliberately indistinguishable (ADR 0004). Chunk read and release ride
  `messages.attachments.readChunk` / `messages.attachments.closeHandle`
  unchanged: the handle, not the addressing, is the single-origin
  authority, so the story bytes need zero new read faces.
- **Engine boundary**: zero engine-repo changes — the red line holds. The
  engine stays metadata-only (E2 projects pointers; bytes never download
  engine-side); the connector's "reuse" of the attachv2 pipeline is the
  connector reusing its own §4.38 CDN mechanism and credential-free
  pointer chain, which the pointer face already names. The face is
  mode-independent by construction (pure connector: store + transit CDN +
  local crypto) — though its practical audience is kt-engine story
  desktops, because only the kt-engine face projects story events.
- **Capability**: the `stories` tag extends in place (the 1.82
  call-signaling no-new-bit precedent); zero new structured error codes
  (`ENGINE_STRUCTURED_ERROR_CODES` unchanged). Second-phase items recorded
  deliberately: media-tier story size profiles (the 100 MiB transit
  ceiling already bounds every caller; a story-specific profile adds a
  second truth without a consumer — still recorded, unworked), and the
  desktop viewer wiring (`story.received` → its store → this face pair) is
  the consumer batch's. The story text/preview-image dispatch item landed
  with contract 1.91 (§4.74): the pointer face is unchanged — the
  url/title metadata surface joined the send face and the receive
  projection carries the engine's `textAttachment.preview` verbatim.

Tests: the shape gate ladder (`service.rs` — the media capability first,
the id/size/locator-cap holes, the account ladder after the parameter
gates, the happy pointer staging with the locator carried verbatim, the
idempotent dedup arm, and the story open face's gate ladder with the
chunk roundtrip); the wire chain (`tests/story_attachment.rs` against a
local mock CDN — the happy pointer lands the plaintext where the story
open reads it and the second call never touches the CDN, the params-gate
and post-parse holes all answer INVALID_REQUEST with zero CDN traffic
including the whole rendering projection passed verbatim, and a digest
mismatch is terminal with nothing landed and no temp residue); and the
schema diff (method enum, dispatch arms, the three param defs, and the
numeric-bound pairs keyed to the code constants).

### 4.72 Cloud-backup upload passthrough: `backups.uploadToCloud`, `backups.uploadStatus` (contract revision 1.87, 2026-10-09)

The engine's cloud-backup upload half (engine `docs/cloud-backup-contract.md`
§6 — the five rulings at §6.0, the measured pins at §6.1, the method faces
and the eight-code table at §6.2, the chain shape at §6.3; the engine-side
method rows live in engine `docs/jsonrpc-compat.md` §3, which names the two
new code positions as this batch's whitelist registration) lands its
connector pipe: two faces plus the connector-owned artifact stage. The
engine's chain (assembly on a dedicated OS thread → backup credential fetch
with the 404 bootstrap self-heal → zkgroup redemption/presentation →
unauthenticated chat gRPC `get_upload_form` with cdn 3 enforced → the TUS
create-with-upload single POST → `refresh_backup`) runs entirely in the
engine's background; the faces start it and observe it.

- **`backups.uploadToCloud`** `{accountId}` → `{status: "started"}`. Send
  lane, account-scoped mutating — the §4.69 restore class verbatim. The
  connector's own stage runs first (the §6.3 chain shape: "connector writes
  the artifact, §10.2 same shape"): the §10.2 transitional artifact
  `backup-export-input.ndjson` is (re)built in the engine's per-account
  data directory with the §4.46 `build_artifact` machine verbatim —
  write-once, rebuilt before every attempt, a failed build removing the
  partial file so the engine never sees a stale one, and the build's own
  failures (registry gap, store, caps, IO) answering with the §4.46
  BuildError faces before any engine traffic. With the artifact landed the
  start forwards `{account}`; the engine's own start gates (registered
  account → artifact present → first line valid → the process-wide
  single-slot gate) then answer their structured codes. A definite
  acceptance answers `{"status": "started"}`; a non-started answer fails
  UPSTREAM_PROTOCOL_ERROR; an indeterminate mutating outcome answers
  SEND_OUTCOME_UNKNOWN and never retries — the terminal state is observed
  through `backups.uploadStatus`, and a retry is the caller's fresh
  `backups.uploadToCloud` (a new form, a new artifact; the server keeps
  the latest successful upload — the §6.2/§6.4 single-shot-no-resume
  semantics, the AGENTS.md unknown-outcome red line). The eight-family
  ladder maps through `cloud_upload_face_error`, the engine's
  `CloudUploadError::rpc_code` table as the single mapping source (shared
  with the failed-phase `errorCode`, so the two ends cannot drift):
  BACKUP_AUTH_FAILED (unregistered/fake account, AEP/registration/identity
  key material missing, zkgroup redemption rejected), BACKUP_NOT_PROVISIONED
  (never provisioned after the bootstrap self-heal),
  BACKUP_ARTIFACT_MISSING / BACKUP_ARTIFACT_INVALID (the artifact gates),
  and BACKUP_UPLOAD_ALREADY_RUNNING (the engine's process-wide single-slot
  re-entry gate — poll the status face instead of re-starting) answer
  retryable=false content-free; BACKUP_TRANSPORT_FAILED (credentials /
  form gRPC / refresh) and BACKUP_UPLOAD_FAILED (non-2xx, cdn≠3, the
  1 GiB cap, uplink read) answer retryable=true with `error.data {detail}`
  verbatim; BACKUP_IO_FAILED (the temp-archive write) answers
  retryable=false with `data.detail` verbatim. **Two per-face divergences
  are reasserted at the upload call site**: BACKUP_IO_FAILED's §4.46
  export face stays retryable=true content-free (a transient disk race the
  desktop may retry after the connector clears) while the upload face is
  contract-fixed retryable=false with detail — the face, not the code,
  owns the face, so the global mapping keeps the export shape and the
  upload path remaps; and BACKUP_TRANSPORT_FAILED keeps its restore text
  on the restore path while the upload path names the upload chain. The
  two new code positions (BACKUP_UPLOAD_ALREADY_RUNNING,
  BACKUP_UPLOAD_FAILED) register in `ENGINE_STRUCTURED_ERROR_CODES` — the
  registration the engine's compat rows assign to this batch; every other
  code the face carries was already whitelisted by its owning batch
  (§4.46, §4.69).
- **`backups.uploadStatus`** `{accountId}` → the engine's progress snapshot
  mirrored: `{present: false}` when no snapshot exists (never started, or
  succeeded — a success leaves no trace, §6.4 —, or the engine restarted),
  else `{present: true, phase: "exporting"|"uploading"|"failed",
  bytesRead, bytesTotal?, errorCode?}`. Read lane, non-mutating (the
  `backups.cloudStatus` discipline). `exporting` is the assembly stage
  (byte counts carry no semantics: the engine holds bytesRead at 0 and the
  total absent); `uploading` is the uplink stage (block counts valid;
  `bytesTotal` is the assembled artifact's byte count — absent only as
  honest uncertainty, never a zero-filled stand-in); `failed` is the
  terminal snapshot, persisting until the next start overwrites it and
  carrying the same structured `errorCode` table as the start face's error
  ladder. An engine that is down (or an indeterminate read) answers
  `{present: false}` — the snapshot is engine-process memory (a
  process-wide single slot), so "not observable" is exactly the
  post-restart truth, not an invented state; a malformed present-snapshot
  face fails UPSTREAM_PROTOCOL_ERROR instead of degrading to
  `{present: false}` (a read that delivered junk knows nothing). The
  answer is rebuilt key-for-key through the projection's shape gate, so no
  unplanned key rides. Deliberately no push event (the §4.68 polling
  precedent).
- **Lane/capability**: `backups.uploadToCloud` is send lane, account-scoped
  mutating; `backups.uploadStatus` is read. The capability set does not
  grow: both faces ride `backup-export`, widened in place (the 1.82
  call-signaling precedent) — the upload's entire input is the
  backup-export artifact chain, and the desktop's backup section already
  gates on the tag; kt-engine only (the §4.69 mode seam,
  CAPABILITY_UNAVAILABLE on the JVM/native faces — the engine methods are
  KT extensions a stock signal-cli does not carry). The schema grows the
  two method enum entries, the two dispatch literals, the two params defs,
  and the two error codes; no schema store version change.
- **Deliberately out of this batch** (recorded, not worked around): the
  media tier and the subscription/level query face (the engine does not
  check levels — the server refuses honestly and the code passes through;
  §6.4), and the desktop settings-page upload entry (the engine contract's
  follow-up batch). No consumption watcher exists or is needed: the upload
  produces nothing local, a success leaves no trace. The §4.46 snapshot
  caretaker and skip-counter merge stay the export face's own — the upload
  creates no snapshot and has no status face to merge counters into, so
  `backup_counters` is deliberately untouched (a later `backups.exportStatus`
  keeps describing the last export build, not an upload's). Artifact
  residue converges by overwrite: the artifact is write-once and rebuilt
  before every attempt (the §4.46 posture), and the engine deletes it only
  on the invalid-first-line hard boundary (§6.2).
- **Fixture**: `fake-signal-cli.py` mirrors the negotiated wire face —
  marker files steer the eight-family ladder
  (`.fixture-cloud-upload-running` = the single-slot re-entry gate;
  `.fixture-cloud-upload-error` = a family sentinel, the three message
  families additionally carrying `data.detail`) and the status states
  (`.fixture-cloud-upload-status` = `<phase> <read> [<total|code>]`), and
  the start arm mirrors the engine's artifact gate honestly: with no
  steering marker, a missing `backup-export-input.ndjson` in the account
  directory answers BACKUP_ARTIFACT_MISSING, so the happy path through the
  real connector pins that the artifact build landed. Real-device upload
  (an enabled-cloud-backup account end to end, the official client reading
  the upload back through restore) is the engine batch's own acceptance
  (§6.5), not this batch's.

Tests: the unit chain pins the whitelist sets (`methods.rs` — both faces in
the read/send/mutating arrays), the eight-code mapping totality with the
content-free/detail split and the two per-face divergences (`service.rs` —
including the pin that BACKUP_IO_FAILED's export face stays retryable=true
content-free while the upload face answers retryable=false with detail),
and the uploadStatus shape gate (`supervisor.rs` — the three-phase set, the
honest absent total, the malformed junk ladder, and the restore phase enum
refused on this face). The fixture chain (`tests/backup_cloud_upload.rs`)
pins the start's connector-owned half (the artifact rebuilt and landed
ahead of the call — a planted stale artifact is never served — with the
§10.2 account line first), the started acceptance, the eight-family ladder
verbatim with the contract-fixed retryability and the detail pass-through,
the three status states, and the mode/account/params gates. The integration
roundtrip (`server_integration.rs::
cloud_upload_faces_round_trip_in_kt_engine_mode`) drives the host IPC end
to end in kt-engine mode: the handshake advertises both faces with no new
feature tag (the upload rides `backup-export`), the started acceptance with
the artifact on disk, the ladder families plus the re-entry gate across the
wire, and the status states. The schema diff is the method enum, the two
dispatch arms, the two params defs, the two error codes, and the `data`
description's third family sentence; the schema-consistency scans cover the
method set, the error-code set, and the unchanged bounds pairs.

### 4.73 Call-link credential material passthrough: `calls.linkPresentation`, `calls.createLinkAuth`, `calls.groupProof`, `calls.groupMembers`, `calls.endorsementsPublicKey` (contract revision 1.88, 2026-10-09)

The engine's call-link credential material face (engine a3ae39b, P4c
slice 1 — the engine-side method rows in engine `docs/jsonrpc-compat.md`
§3 calls family and the architecture block at §5, whose
credentials-never-ride-host-HTTP ruling the connector cites as the
authoritative shape) lands its connector pipe: five faces, pure
passthrough — zero logic, zero parsing, zero logging. The engine owns the
whole zkgroup chain (the per-account double-family credential cache with
the official groupCredentialFetcher window cadence, the redemptions, the
presentations, the group-state reads); the connector is the pipe between
the engine's materials and the desktop + calling sidecar's RingRTC
consumer.

- **Shape gate → engine method direct map → result passthrough**: each
  face gates its material shapes locally (the engine's own fail-closed
  parameter gates mirrored onto the caller strings — trim, canonical
  padded standard base64, exact decoded byte length: rootKey 16 B (V0) or
  21 B (V1), the RingRTC `CallLinkRootKey` two-version lengths whose
  widest padded encoding is 28 characters; roomId 32 B, the RingRTC
  `derive_room_id` HKDF-SHA256 fixed output (44 characters); groupId 32 B,
  the same group master key the listGroups/terminateGroup faces address
  (44 characters)), answering INVALID_REQUEST retryable=false before any
  upstream call — an engine -32603 would degrade to the generic retryable
  upstream error, the wrong face for a caller mistake. Past the gate the
  connector resolves the account (ACCOUNT_NOT_FOUND before the seam) and
  forwards `{account, ...material}` verbatim; the engine's result bytes
  pass through untouched. The decoded material bytes are dropped at the
  gate (the caller's string forwards — the engine re-gates fail-closed
  anyway); nothing is parsed, logged, or persisted anywhere on the pipe.
- **The five faces**:
  `calls.linkPresentation` {accountId, rootKey} → `{presentation}` — the
  day's CallLinkAuthCredentialPresentation bincode bytes, standard base64
  (the official zkgroup.preload presentation chain; the desktop/calling
  RingRTC binding consumes the decoded bytes);
  `calls.createLinkAuth` {accountId, rootKey, roomId} →
  `{presentation, callLinkPublicParams}` — the create-auth two-shot inside
  one RPC (POST v1/call-link/create-auth → redemption → presentation +
  public params, the official createCallLink credential segment's two
  materials);
  `calls.groupProof` {accountId, groupId} → `{proof}` — the
  ExternalGroupCredential token string (GET v2/groups/token on the
  storage-service host; RingRTC `setMembershipProof` consumes its UTF-8
  bytes);
  `calls.groupMembers` {accountId, groupId} → `{members:
  [{uuid, userIdCipherText}]}` — the group state's members with each ACI
  encrypted into its UuidCiphertext (server group-state order; the
  plaintext identities never leave the engine);
  `calls.endorsementsPublicKey` {} → `{endorsementPublicKey}` — the
  constant derivation from the engine's pinned legacy ServerPublicParams;
  the one account-less face (the getStickerPackManifest anonymous-face
  discipline), served by the default group's engine.
- **Lane/class**: all five faces read lane, non-mutating — network
  possible inside one bounded engine call (the conversations.getAvatar
  class); createLinkAuth's server-side POST is credential issuance with no
  user-visible state change, so no write lane and no SEND_OUTCOME_UNKNOWN
  exists on any of the five (a timeout answers UPSTREAM_TIMEOUT,
  retryable). The engine call class is read-only throughout.
- **Error ladder**: the two new code positions whitelist-registered in
  `ENGINE_STRUCTURED_ERROR_CODES` (the registration the engine's compat
  rows assign to this batch): CALL_CREDENTIALS_FAILED (retryable=false —
  server rejection, zkgroup redemption/presentation verification failure,
  response shape violation, the day's credential absent;
  `error.data.status?` carries the server HTTP status verbatim when the
  server gave one — a promised contract face riding like the
  PROFILE_UPLOAD_FAILED facts, with the engine's empty no-status object
  passing through as itself) and CALL_CREDENTIALS_UNAVAILABLE
  (retryable=true — the network/transport family, content-free). Shape
  gates answer INVALID_REQUEST locally; an engine account that is unknown
  to the engine degrades per the generic ladder (-32603 → UPSTREAM_ERROR),
  RUNTIME_NOT_RUNNING when the engine is down.
- **Capability/mode**: the capability set does not grow — all five ride
  `call-signaling`, widened in place (the §4.49/1.82 no-new-bit precedent:
  the faces are the signaling pipeline's own material supply). kt-engine
  only (CAPABILITY_UNAVAILABLE on the JVM/native faces — KT-extension
  methods a stock signal-cli does not carry, the §4.69 mode seam; the
  ladder order is shape → resolution → seam, the §4.67 precedent). The
  schema grows the five method enum entries, the five dispatch arms, the
  five params defs (the b64 fixed-length fields' descriptions pinning the
  byte budgets), the two error codes, and the `data` description's fourth
  family sentence; no schema store version change.
- **Deliberately out of this batch** (recorded, not worked around): the
  relay frame assembly and the RingRTC consumption (kt-signal-calling,
  slice 3+); rootKey generation, roomId derivation, and adminPasskey
  generation stay RingRTC-side (the engine only receives finished bytes);
  group-call connection/SFU/media faces (out of the engine's boundary
  too); any credential caching connector-side (the engine's cache is the
  single one); call-state registration (the connector stays the honest
  `connector-pipe` registry, §4.49).
- **Fixture**: `fake-signal-cli.py` mirrors the negotiated wire face —
  the account-scoped four gate the account and their material shapes
  fail-closed -32603 (the engine's own gates), endorsementsPublicKey
  answers unconditionally; `.fixture-call-cred-failed` answers
  CALL_CREDENTIALS_FAILED (`status <N>` content carrying data.status,
  anything else the empty no-status object) and
  `.fixture-call-cred-unavailable` answers CALL_CREDENTIALS_UNAVAILABLE;
  the happy paths answer fixed shape stubs (placeholder bytes, proof, one
  member — stub bytes are NOT valid zk material, they pin the contract
  shape, the engine fake discipline). Real-device zkgroup material (a
  registered account producing verifiable presentations/proofs end to
  end) is the engine batch's own acceptance, not this batch's.

Tests: the unit chain pins the whitelist sets (`methods.rs` — the five
rows in the read array, non-mutating), the shape-gate ladder with the
verbatim forwarding and the mode seam (`service.rs` — both legal rootKey
lengths, every illegal decoded length and non-canonical encoding, the
roomId/groupId 32-byte gate, shape-before-resolution-before-seam), and
the two-code mapping with the status pass-through faces (`service.rs` —
data.status riding verbatim, the empty no-status object riding, the
unavailable family content-free; the allowlist totality loop covers both
codes mechanically). The fixture chain (`tests/call_credentials.rs`) pins
the five happy pass-throughs byte-for-byte against the stub shapes, the
two-code ladder across the wire, the local INVALID_REQUEST shape answers
(an engine -32603 would have surfaced as the generic upstream error, so
the code is the connector's own voice), the ladder order, and the mode
seam. The schema-consistency scans cover the method set, the error-code
set, and the five new bounds pairs (rootKey ×2 = 28, roomId = 44, groupId
×2 = 44).

### 4.74 Story link-preview metadata: `messages.sendStory` `previewUrl`/`previewTitle` + the receive projection's verbatim preview (contract revision 1.91, 2026-10-09)

The §4.71 second-phase item "story text/preview-image dispatch" lands its
metadata half: a story's link preview is no longer pointer-only. The
caller names the url and title on the send face, and the receive
projection — audited, not widened — already carries whatever the engine
projects under `textAttachment.preview` verbatim. The official anchors:
the story textAttachment's preview mirrors the message link preview
(signal-desktop `ts/protobuf/SignalProto.ts` TextAttachment.preview, AGPL
read-only anchor), and the official `isValidLink` gate is https-only for
the url face — the connector mirrors it fail-closed on the send side.

- **`messages.sendStory` grows two independently optional keys** (every
  existing key byte-identical, `previewImage` behavior unchanged):
  `previewUrl` — when present it must be non-empty, at most 2048
  characters (the §5.10 url budget mirrored), and start with `https://`
  (the official `isValidLink` posture; `http://` answers INVALID_REQUEST,
  unlike the §5.10 message link-preview face which still admits both
  schemes upstream); `previewTitle` — when present it must be non-empty
  and at most 1024 UTF-8 bytes (the §5.10 title budget mirrored). Either
  key may ride alone; absence (or null) means the key is never sent — no
  empty-string placeholder reaches the engine frame. Every violation
  answers the deterministic INVALID_REQUEST retryable=false ahead of the
  account ladder and the mode seam (the §4.67 validation-first order): a
  violating value is never silently dropped and never forwarded. Past the
  gates both values ride the engine frame verbatim as the camelCase keys
  `previewUrl`/`previewTitle` beside the existing `previewImage` pointer
  key — the parallel engine batch composes the proto
  `Preview{url,title,image}` and owns the `project_story_message`
  `preview.url`/`preview.title` projection (Some-non-empty only, absent
  key otherwise); this repository forwards what the contract pins.
- **Receive/verify: the chain was already a verbatim pipe — pinned, not
  widened.** The story receive chain is a passthrough at every hop: the
  engine envelope's `storyMessage` object is bounded (serialized ≤ 3 MiB,
  `bounded_story_material` — a whole-object clone, the only guard) but
  never reshaped (src/engine.rs), rides `ControlReceive::Story` → the
  transient `HostSideEvent::StoryReceived` after the stories-disabled
  gate (src/service.rs), and forwards as the `story.received` event's
  `story` object byte-for-byte (src/host.rs). There is no key whitelist
  to widen anywhere on the chain, so the engine-side 1.91 sub-keys reach
  the desktop naturally. Regression tests pin the new sub-keys — plus an
  unnamed sibling key proving the no-whitelist posture — at three points
  (the engine normalization, the service event, the host wire frame), so
  a future reshape cannot strip them silently.
- **Boundary**: zero engine-repo changes on this side (the engine's own
  batch owns `sendStory`'s new params and the projection keys); the
  JVM/native seam still answers CAPABILITY_UNAVAILABLE after validation
  (the §4.67 mode seam); no new method, event, error code, or capability
  (`stories` widened in place, the 1.82 no-new-bit precedent); no store
  version change. The schema grows the two `messagesSendStoryParams`
  properties (maxLength pairs keyed to the new
  `MAX_STORY_PREVIEW_URL_CHARS`/`MAX_STORY_PREVIEW_TITLE_BYTES`
  constants, the url `pattern` `^https://` pinning the prefix) and the
  method description's metadata sentence.

Tests: the unit gate ladder with the verbatim ride and the independent
absence arms (`service.rs` — the happy keys composed with the unchanged
`previewImage`, non-https/empty/over-cap url holes, empty/over-cap
multibyte title holes, gate-before-account ordering, the mode seam after
the gates, url-alone/title-alone frames carrying no sibling key); the
engine-side verbatim normalization pin (`engine.rs` — preview url/title
plus an unnamed key all ride the story control); the service event pin
(`service.rs` — `StoryReceived` carries the preview metadata verbatim);
the host wire pin (`host.rs` — the `story.received` frame carries
`story.textAttachment.preview.url`/`.title` byte-for-byte, absent
identities stay absent); the supervisor wire chain (`supervisor.rs` — the
full-option sendStory dispatch through the fake engine carries the
camelCase keys, the lean frame stays free of them, and the pre-existing
lean/full assertions keep `previewImage` behavior pinned); and the schema
diff (the two maxLength pairs + the `pattern` in
`tests/schema_consistency.rs`).

### 4.75 Group timer-change timeline row: the `groupUpdated` receive face lands the official notice row (contract revision 1.92, 2026-10-09)

The 2026-10-06 engine batch (recorded in the §4.44 dated note, whose posture this section
overturns) left the group timer change's presentation an open boundary: the mirror converged
(`conversations.expire_timer_seconds`, the refreshed summary riding `conversation.changed`)
but no row landed, so the group timeline had nothing to render. The official client renders
a group timer change as the same timer-notice system row a direct-conversation change lands
(§4.41); this revision lands that row beside the mirror, and the desktop batch of the same
date retires its compensating toast with it.

- **Row shape**: exactly the §4.41 row (src/service.rs). The §4.41 material rides `text`
  verbatim with its byte count — "已更新消息定时消失：{N} 秒", the close form
  "已更新消息定时消失：已关闭" for 0 — the §4.41 rich marker (`is_expiration_update`) is
  the row's only rich content, `direction`/`status` are `system`, the row carries no expire
  columns (a notice row never sweeps), the host preview is the 120-char face every system
  row takes, and `increment_unread` stays false (the official notice narrates in-timeline
  without badge pressure).
- **Attribution ladder** (the row's sender face): the projection's `editor` — the change
  author's ACI, bounded to 64 bytes at the engine seam (`GROUP_UPDATED_EDITOR_MAX_BYTES`; a
  hostile or overlong value reads as absence, never truncated into a fake id) — resolves
  through the recorded ACI→peer-key mapping first (the §4.37 identity table), so an editor
  the envelopes already introduced joins the desktop roster by its number form; an unmapped
  ACI rides raw (the roster joins it by uuid); no editor at all keeps the
  conversation-scoped unattributed sender (the stable hash form the pre-row era used) — the
  official unknown-member branch, never a guessed identity. `sender_id` carries the chosen
  address un-hashed: the desktop resolves the actor from its roster join (the official
  `fromMember` posture), and that join keys on the raw address.
- **Identity, dedup, guards**: the row identity is `(system, sent_at, sender_id)` with the
  legacy-sender twin admitted (`message_by_signal_identity`), so the protocol timestamp is
  load-bearing — a stamp-less or zero-stamped envelope keeps the mirror and lands no row (a
  locally invented stamp would turn a replay into a second row). A same-value event still
  lands its row: the official row narrates the change envelope, not the diff, and a member
  re-setting the same value is a real change upstream (the group revision advanced); the
  summary still re-pushes so a lagging window converges. A replayed envelope (same identity
  tuple) dedupes through the row identity and converges nothing a second time.
- **Ordering**: the mirror lands before the row, so the `conversation.changed` summary the
  same ingest emits already states the new timer; the events ride
  `[message.upserted, conversation.changed]`.
- **Boundary**: zero schema shape change (the row reuses the §4.41 fields; the schema's
  receive description sentence is corrected the same revision — the 2026-10-06
  state-convergence-only posture is superseded); no new method, event, error code, or
  capability; no store version change. Recorded seam, untouched: the JVM/native dataMessage
  timer-notice path (signal-cli's native `isExpirationUpdate`) already landed rows before
  this batch and keeps its sender as the bounded legacy hash — joining that sender to the
  roster is the pre-existing §4.41 gap, deliberately not this batch's scope.

Tests: the mirror+row unit (`service.rs` — the row's conversation/direction/status, the
material verbatim for the 3600 form and the close form, the rich marker, no expire columns,
the unattributed sender hash for the editor-less envelope, the summary carrying the new
timer, the off form storing 0 literally, the same-value envelope landing its row, and the
replayed envelope deduping to an empty event list); the attribution unit (`service.rs` —
the mapped ACI resolving to its number-keyed peer, the unmapped ACI riding raw, the
stamp-less envelope keeping the mirror without a row); the engine projection pin
(`engine.rs` — the editor rides the `GroupTimerUpdate` control, hostile and overlong editor
forms read as absence); and the unknown-group drop test (warn-only, no row invented).

### 4.76 Adjacent visual-media probe: `messages.adjacentMedia` + the `message-adjacent-media` capability (contract revision 1.93, 2026-10-09)

The desktop lightbox's cross-message navigation (the KT desktop L4 parity item) needed a
history face the contract did not have: the official client opens the lightbox with
`getConversationRangeCenteredOnMessage` for its has-prev/has-next boundary and steps across
message boundaries with `getOlder/NewerMessagesByConversation` under
`requireVisualMediaAttachments` (Signal-Desktop `ts/state/ducks/lightbox.preload.ts`
`showLightbox`/`showLightboxForAdjacentMessage`, SQL in `ts/sql/Server.node.ts`
`getAdjacentMessagesByConversation` — predicate `hasVisualMediaAttachments IS 1 AND
isViewOnce IS 0`, keyset on `(received_at, sent_at)`, anchor excluded via
`requireDifferentMessage`). `messages.list` only pages a window backwards from a cursor; it
cannot answer "the next media-bearing row after this anchor" and carries a
thread-viewing side effect the probe must not have. This revision lands the probe as a
first-class read-lane method instead of stretching the paging face.

- **Method**: `messages.adjacentMedia` (read lane; advertised through the new
  `message-adjacent-media` capability tag). Params `{accountId, conversationId,
  anchorMessageId, direction, limit}`; result `{items: MessageRecord[]}` in the
  `messages.list` stored shape (host projection, reactions and edits attached the same
  way), ordered adjacency-first — the row closest to the anchor first, so `limit: 1` is
  exactly the official single-step probe and larger limits stay meaningful for strip
  preloading.
- **Anchor semantics**: the anchor is a message id the caller already holds (the current
  lightbox item's message). It resolves to its stored `sent_at`; a vanished anchor answers
  INVALID_REQUEST retryable=false through the store's `InvalidCursor` rung — the official
  probe anchors on a live row too, and a silent re-anchor would invent a position the
  caller never asked from. The anchor row itself is never returned (the official
  `requireDifferentMessage` posture), and the scan's `(sent_at, id)` keyset tiebreak matches
  the `messages.list` ordering so the two faces never disagree about adjacency.
- **Strip predicate**: a row matches when at least one attachment carries an `image/*` or
  `video/*` content type — the desktop lightbox strip's own bar
  (`signalAttachment.ts`) — and view-once rows are always excluded (the §4.37 boundary:
  tap-to-view media never joins a navigation strip; both `viewOnce` and
  `view_once_invalid` rich forms count). Story rows need no predicate: a story never lands
  in the messages table at all (§4.67), so the probe cannot leak one. The filter runs in
  Rust over bounded keyset chunks (`ADJACENT_MEDIA_SCAN_CHUNK` = 128) because the store has
  no JSON1 dependency to push it into SQL; the scan stops as soon as `limit` matches
  accumulate or the range exhausts, so a sparse conversation never scans more than one
  chunk past the last match.
- **Bounds and lanes**: `limit` is clamped 1..=`MAX_ADJACENT_MEDIA_LIMIT` (20) at the host
  gate; the account/conversation ladder is the same deterministic rung order as
  `messages.list` (AccountNotFound, then ConversationNotFound). Store-only read: no
  upstream traffic, no engine involvement (the JVM/native seam has no mode for this — the
  probe is a pure local-database answer), and deliberately **no** unread-clearing side
  effect — `messages.list` clears the badge because it is the thread view; probing the
  lightbox's neighbors is not viewing the thread, so the badge stays put.
- **Boundary**: zero schema shape change beyond the new method arm and params definition;
  no store version change; no new error code (INVALID_REQUEST covers the param gate and
  the vanished anchor); no event. An empty `items` array is the honest
  no-more-media-in-this-direction answer — the desktop maps it to the official
  `UnableToLoadAttachment` toast at the boundary, never to a fabricated item.

Tests: the store units (`store.rs` — both directions from a mid-timeline anchor over a
mixed strip (image/video match; text, audio, pdf, and untyped attachments skip; view-once
image excluded; another conversation's media invisible; limit truncation; the empty
range; a missing anchor raising `InvalidCursor`), the `(sent_at, id)` tiebreak walking
same-timestamp rows in the probe direction, and a scan crossing more than one bounded
chunk to reach the next media row); the service unit (`service.rs` — the
account/conversation ladder rungs, the probe answering the media row while the
conversation's unread badge stays put, and the vanished-anchor INVALID_REQUEST rung); and
the read-lane registration pin (`methods.rs`).

### 4.77 Viewed-state projection (`viewedAt`) + story-anchored reaction send `stories.sendReaction` (contract revision 1.94, 2026-10-10)

Two desktop surfaces needed connector truth the contract did not carry. The voice-note
PlayedDot (the official `isVoiceMessagePlayed`: incoming `readStatus === Viewed`,
outgoing `sendState.isViewed` — Signal-Desktop `ts/util/isVoiceMessagePlayed.std.ts`,
read-only behavior reference) had no projected key: the VIEWED receipt was already routed
(pinned signal-cli 0.14.8 `JsonReceiptMessage {when, isDelivery, isRead, isViewed}`,
engine `ReceiptKind::Viewed`) but only folded onto the read tier, and
`messages.markViewed` fanned the upstream receipt out without remembering anything
locally. The story AnimatedEmoji send had no face at all: `messages.sendReaction`'s
row-addressed ladder can never target a story (rowless, §4.67).

- **Row projection `viewedAt`** (store schema 20 → 21, additive nullable column, never
  backfilled): on outgoing rows, the receipt envelope timestamp of the first peer VIEWED
  receipt that hit the row (the official `sendState.isViewed`); on incoming rows, the
  connector wall-clock ms the host's `messages.markViewed` first selected the row (the
  official `readStatus === Viewed` — this device played it). First stamp wins on both
  faces; the key stays absent on rows never viewed and on every pre-1.94 row.
- **Outgoing face**: `upgrade_outgoing_receipts` now answers
  `{moved, viewed_confirmed}`. A VIEWED receipt rides the read ladder unchanged and
  additionally first-stamps every hit row in the read-upgrade eligibility set
  (`pending|sent|delivered|read` — failed rows never take the stamp): moved rows carry
  the stamp on their `message.statusChanged` (the event data gains the optional
  `viewedAt`), an already-read unstamped row emits the same event with the status kept
  `read` (the stamp-only transition, the row's existing stamps re-carried), and a pure
  replay (already read, already stamped) emits nothing — the §4.70 story split's
  hit-test keeps its "row exists ⇒ message receipt" rule, so the stamp-only event never
  leaks a story candidate. Delivery and read receipts never stamp.
- **Incoming face**: `messages.markViewed` first-stamps the selected incoming rows at
  `now_ms()` before the upstream fan-out — the official local-first discipline, so the
  played marker is the request's own local effect: it lands even when the author group
  list is empty (group conversations stamp without any upstream receipt), survives an
  upstream failure (a receipt-class `unknown` never un-plays a row), and never
  re-stamps. `messages.markRead` keeps its no-local-write discipline (the desktop owns
  its read state, §4.29).
- **`stories.sendReaction`** `{accountId, storyAuthor, storySentAt, emoji, remove?,
  operationId?}` → `{status: "sent"|"unknown"}`. Send lane, account-scoped mutating; the
  `messages.sendReaction` send class verbatim — a definite upstream error propagates,
  `unknown` is never retried. The story anchor comes verbatim from the caller (the
  desktop's story timeline owns both halves); the frame is the pinned upstream's own
  story-reaction face (pinned signal-cli 0.14.8 `sendReaction --story`, "React to a story
  instead of a normal message"; engine-side `Manager.sendMessageReaction(..., isStory)`):
  `{account, emoji, remove, targetAuthor, targetTimestamp, recipient: [storyAuthor],
  story: true}` — the official client delivers a story reaction as a direct message to
  the story's author. Nothing is written locally and no event is emitted (a story
  reaction has no row to pill on). v1 addresses author-direct reactions only: a
  group-story reaction needs the group context the `story.received` projection does not
  carry, so there is nothing to address a group frame with — a recorded boundary, not an
  invented one.
- **Mode seam**: JVM/native dispatch the frame as-is. The kt-engine face's `sendReaction`
  has no story half and silently ignores unknown params (it would mis-send a plain
  reaction), so it answers CAPABILITY_UNAVAILABLE (retryable=false) after the same local
  validation — the §4.53 engine-gap precedent, closing when the engine's own batch lands.
  The capability set does not grow: the `stories` tag extends in place (the 1.82
  call-signaling precedent).
- **Recorded upstream limits** (verified against the pinned 0.14.8 distribution before
  anything was exposed): the own-device viewed sync has no projection source — the
  pinned `JsonSyncMessage` maps `Sync$Read` to `readMessages` but never projects
  `MessageEnvelope$Sync$Viewed`, and the connector consumes neither, so a voice note
  played on a linked second device does not stamp `viewedAt` here (documented, not
  fabricated); the kt-engine story-reaction debt above is the second. Both close when
  their upstreams grow the face.

Tests: the store units (schema v21 lands the column without backfilling; the VIEWED fold
across the ladder — read/delivery receipts never stamp, a moved row stamps inside
`moved`, the already-read row surfaces in `viewed_confirmed`, a pure replay emits
neither, a failed row never stamps, first stamp wins; `stamp_incoming_viewed` stamps
exactly the selected incoming rows, skips outgoing rows in the list, and a restamp is a
no-op); the service units (the markViewed local half — explicit/default selection,
group-conversation stamping with an empty fan-out, first stamp wins, markRead stamps
nothing; the VIEWED event projection — moved rows carry `viewedAt`, the already-read
confirmation keeps `read` and re-carries the row's stamps, the replay stays silent, and
the stamps persist on the row projection; the `stories.sendReaction` gates — author
bounds, zero storySentAt, the emoji shape, an oversized accountId, an unknown account —
then the JVM/native frame shape with `story: true` and the trimmed author, `remove`
verbatim, and the kt-engine CAPABILITY_UNAVAILABLE after validation); and the supervisor
roundtrips (JVM mode — `sent`, the 423 crash sentinel answering `unknown` without a
retry, the explicit restart and the next reaction sending again, the dispatch log
pinning `story: true` with the store account id never riding; kt-engine —
CAPABILITY_UNAVAILABLE with no frame leaving the process; the markViewed end-to-end —
the upstream receipt confirming `sent` while the rows stamp locally, the group
conversation's trivial `sent` still stamping, and markRead stamping nothing).

### 4.78 Chat-folder sync passthrough: `chatFolders.list` + `chatFolders.setRecords` (contract revision 1.95, 2026-10-10)

The desktop's folder picker needed the pinned upstream's chat-folder surface, the
kt-engine already had both verbs real'd (the engine's `docs/jsonrpc-compat.md` §5
Storage-record face) — the connector contract simply did not carry them. Both land as
pure passthrough: a chat folder record is the engine's own shape and the connector never
re-derives, re-keys, or stores it. The record grammar (verified against the engine's
storage records, mirrored by the test fixture's strict shape gate): `id` (uuid string),
`name`, `position` (u32), `showOnlyUnread`, `showMutedChats`,
`includeAllIndividualChats`, `includeAllGroupChats`, `folderType`
(`UNKNOWN`|`ALL`|`CUSTOM`), the optional `includedConversationIds` /
`excludedConversationIds` lists (entries `{conversationId, kind:
contact|group|legacyGroup}`), and `deletedAtTimestampMs` (u64 — always present on the
read face, `0` live, non-zero tombstone).

- **`chatFolders.list`** `{accountId}` → `{folders: [...]}` (the `ChatFolderSyncs`
  struct). Read lane, non-mutating, metrics `read`. The upstream answer's `folders`
  array rides verbatim (`Vec<Value>`: keys, entry shape, and ordering untouched, in the
  engine's manifest first-appearance storage order), entries without a usable identity
  (a string `id`) dropped (the §4.34 discipline), the array capped at
  `MAX_CHAT_FOLDERS` (64). The engine's no-master-key / unknown-account face answers
  the honest empty array.
- **`chatFolders.setRecords`** `{accountId, records?, removeIds?}` → the same
  `{folders: [...]}` projection — the engine answers with the post-write manifest, so
  the caller sees the resulting set without a second read. Send lane, account-scoped
  mutating, metrics `send`, the reaction class: a definite upstream error propagates,
  an unknown outcome answers SEND_OUTCOME_UNKNOWN (retryable=false) and is never
  retried. Local gates (all INVALID_REQUEST, retryable=false): at least one half
  non-empty (the accounts.updateConfiguration rule), each half at most
  `MAX_CHAT_FOLDER_BATCH` (64), every `records` entry an object carrying a non-empty
  string `id` of at most `MAX_CHAT_FOLDER_ID_CHARS` (128), every `removeIds` entry a
  non-empty string of at most 128 chars — the identity face only; the folder grammar
  stays the engine's audit, never a local re-derivation. The upstream frame carries
  only the non-empty half (`records` / `removeIds` keys appear as needed), values
  verbatim.
- **Zero local footprint**: no record content lands in the connector store — no
  column, no row, no cache; the cloud record is the whole state. The store's account
  id never rides the wire (resolved to the upstream `account` at prepare, the §4.33
  discipline), and the resolved id is the only key the frame adds.
- **Idempotency is the engine's own documented semantics** (upsert by record `id`, a
  non-zero `deletedAtTimestampMs` as tombstone, `removeIds` as the official deleteKey
  face, a same-value rewrite skipping the revision bump): the connector forwards and
  observes, it does not re-implement — a repeated identical write is the engine's
  no-bump no-op and the answer is the same manifest either way.
- **Mode seam — kt-engine only**: stock signal-cli carries no `chatFolders.*` verb, so
  the JVM/native modes answer CAPABILITY_UNAVAILABLE (retryable=false) after the local
  gates — the §4.53 engine-gap precedent mirrored in reverse (there the kt-engine
  lacked the face; here the stock engine does). The ladder order holds (shape gates →
  resolve_account → mode seam: a bad record with an unknown account still answers the
  shape, an unknown account with a clean frame answers ACCOUNT_NOT_FOUND before the
  seam speaks) and the supervisor prepares under the service lock before
  `running_engine`, so the seam answers before any engine starts. The capability set
  grows by one: `chat-folders` joins `FEATURE_CAPABILITIES` and the handshake
  advertisement (the 1.36 `conversation-pin-sync` precedent — a face the stock engine
  cannot serve is gated on its own capability token).
- **Error face — transparent, no new codes**: the engine's own errors ride the wire —
  STORAGE_WRITE_FAILED / STORAGE_READ_FAILED / STORAGE_UNAVAILABLE (already in the
  allowlist, the schema, and the service mapping, retryable=true — the engine owns the
  family's final set) and the engine's `-32603` (a stale engine without the verb
  registered, malformed params) folding to UPSTREAM_ERROR (retryable=true) through the
  existing `into_api` face.

Tests: the service units (`prepare_chat_folders_gates_and_forward_the_verbatim_batch` —
the batch caps on both halves, the records identity face and the removeIds bounds, the
at-least-one rule, the resolved non-empty-half frame with the store account id never
riding; `chat_folder_syncs_result_caps_and_passes_through` — the 64-entry cap, the
`id`-less drop, contents byte-equal); the methods classifications (`chatFolders.list`
in the read lane and the non-mutating set, `chatFolders.setRecords` in the send lane
and the account-scoped mutating set, the `chat-folders` advertisement); the supervisor
roundtrips (`tests/chat_folders.rs`: `chat_folder_faces_pass_through_verbatim` — the
fixture's two eleven-key records (an ALL-type and a CUSTOM carrying all three
recipient kinds) read byte-faithfully, a records-only write crossing verbatim and
reading back the post-write manifest with the `removeIds` key absent, a removeIds-only
write reading back one folder with the `records` key absent;
`chat_folder_error_ladder_rides_the_wire` — the fixture's failure markers landing
STORAGE_WRITE_FAILED on the write, STORAGE_READ_FAILED on the read,
STORAGE_UNAVAILABLE on both; `chat_folder_gates_answer_locally` — the at-least-one
rule, the non-object / `id`-less / empty-`id` / non-string-`id` entries, the 129-char
ids on both halves, the 65-entry batches, the shape-over-resolution and
resolution-over-seam ladder, and the engine-unknown account degrading to
UPSTREAM_ERROR; `chat_folder_crash_answers_unknown_and_never_retries` — the crash
sentinel answering SEND_OUTCOME_UNKNOWN (retryable=false) with exactly one dispatch
frame in the log; `chat_folder_faces_gate_the_mode_seam` — JVM answering
CAPABILITY_UNAVAILABLE on both faces before any engine starts, not one frame logged);
plus the schema-consistency bound pairs and the server handshake advertisement.

### 4.79 Read-only diagnostic face: `runtime.doctor` (contract revision 1.97, 2026-10-10)

The desktop's support surface needed one call that answers "what is actually going on
inside this connector" without being able to change anything by asking: the whole
diagnostic payload rides one Control-lane method with `{}` params exactly (anything else
answers INVALID_REQUEST, retryable=false — no new error codes, no new capability token,
the method rides the handshake method-list advertisement), and the discipline is
read-only end to end: the doctor never starts or stops an engine, never breaks, removes,
or truncates a lock, never writes a byte — every probe reads, and a slice that cannot
answer degrades honestly while the call still succeeds. The answer is
`{connector, groups[], network, recentErrors, service}`; since contract revision 1.101
each group additionally carries the `credentials` face (§4.82), the doctor's one
account-authenticated probe; the field-level consumer reference lives
in `docs/doctor-contract.md`.

- **Version chain** — `connector` carries the connector's own `version`, `apiVersion`,
  and `pid`; each group's `engine` carries the launcher-selected `mode` (the clap value
  name, so the label follows the enum verbatim), the live `state`/`pid`/`rssBytes`/
  `resourcePressure` status view, the doctor's own `reachable` probe result, and the
  engine method faces: `version` and `engine.protocolCapabilities`, plus (since 1.133,
  §4.116) `perfMetrics` — the engine's read-only snapshot through the connector's
  allowlist projection, not verbatim. A face is mutually
  exclusive by construction — `present: true` with the engine's own answer riding
  through **verbatim** (never re-derived, re-keyed, or normalized; the kt-engine
  self-certification face keeps its upstream spelling, `profiles_v2` included;
  `perfMetrics` is the one deliberate exception — its `face` is the allowlist
  projection with `connectorDroppedKeys`), or
  `present: false` with a static, content-free `reason` — never a value the connector
  invented.
- **Honest degradation** — an engine that is not running answers `reachable: false` and
  the method faces absent with the state's static reason (stopped: not started since connector
  boot or stopped by runtime.stop; exited: the process ended on its own; faulted: it
  crashed or failed to start, the watchdog may restart it); a running engine whose live
  handle vanished mid-probe answers the transitioning reason; a probe that exhausts the
  engine-call budget (3 s — a wedged engine must not hold a diagnostic anywhere near the
  ordinary 15 s request timeout), hits engine backpressure, or watches the engine exit
  mid-probe answers `the engine stopped answering mid-probe`; an engine error answers
  the per-method error-class reason (for the self-certification face: a stock signal-cli
  engine does not carry the kt-engine face). A diagnostic never fails as a whole because
  one engine face could not answer.
- **Data-dir face** — `path`/`exists`/`writable`/`freeBytes`/`totalBytes` from a probe
  that writes nothing: `stat`, `access(2)` `W_OK` for the directory's own writability
  (Windows falls back to the read-only attribute — best effort there, pending a Windows
  test host), and `statvfs(2)` for the free/total pair (absent when the directory does
  not exist). The path rides to the authenticated host on purpose — the launcher passed
  it at spawn, so the peer already knows it — and never enters logs.
- **Lock face** — the `.kt-signal-connector.lock` occupancy classified
  `none`/`held`/`stale`/`released`: `present` is the file's existence, `held` the kernel
  flock probe (open read-only, `LOCK_EX|LOCK_NB`, unlock immediately — a `held` verdict
  means someone else holds it, and the doctor never waits for, breaks, or removes a
  lock), `pid` the decimal pid the connector wrote into the file at acquire time
  (absent for pre-face files or corrupted content), `holderAlive` true by kernel
  guarantee under `held`, else the recorded pid's liveness when known, `stale` a
  dead-pid remnant, and `released` the designed post-release remnant (the lock file is
  deliberately never deleted, so this is the normal state after a clean connector exit).
- **Recent-error ring** — `recentErrors` answers with the fixed `window` (64), the
  process-lifetime `recordedTotal`, and the newest-64 aggregation by family,
  count-descending: `source: "host"` families are structured dispatch error codes with
  their retryability, `source: "protocol"` families are engine protocol warning kinds,
  `lastAtUnixMs` the newest event's unix-millisecond stamp. The ring is in-memory only
  and content-free by construction — codes, kinds, counts, timestamps, never message
  content.
- **Network face** — the existing `watchdogRestartsTotal` and `receiveDroppedTotal`
  counters read out as-is; the doctor adds no new network instrumentation.
- **Signal service-plane face** — `service` answers `proxyConfigured` (the launcher's
  `KT_SIGNAL_SOCKS_PROXY` process environment present and well-formed; the per-group proxy
  plans are deliberately not consulted — the probe reflects the process-wide launch
  environment, so a flag-only proxy reads as unconfigured) and two probes, `chat`
  (`GET https://chat.signal.org/v1/accounts/whoami`, unauthenticated — the desktop reads the
  link healthy at the service edge's own 401) and `storage` (`GET https://storage.signal.org/`
  — any HTTP response is a healthy link). The client is built fresh per diagnostic: HTTPS
  only, the vendored Signal production root CA as the only trust anchor
  (`tls_built_in_root_certs(false)` — the kt-engine's tls_certs_only attitude, so the probe
  validates exactly the TLS reality the engine's own traffic will meet), the launcher proxy
  as a `socks5h://` route (remote DNS, matching the engine's `ALL_PROXY` mapping), 3 s
  connect and overall budgets (the same diagnostic bound as the engine probes), redirects
  off. Every outcome folds into exactly one fixed literal — `http` (the edge answered; the
  only classification carrying `status` and `latencyMs`), `timeout` (the diagnostic budget
  exhausted), `socks_refused` (a connect-stage failure inside the SOCKS hop, identified by
  reqwest's own socks-layer marker in the error's source chain; the residual transport
  failures fold here rather than inventing a sixth literal), `tls_failed` (the handshake ran
  against the pinned CA and failed — a trust mismatch is distinct from a dead route), and
  `not_configured` (no proxy environment — no request is attempted at all). The mapping is a
  pure function over reqwest's own kind flags (`is_timeout`/`is_connect`) plus the
  source-chain marker walk, pinned by unit tests with no network dependency. The red line
  stands: the report carries only the fixed literals plus a status code and a latency —
  never upstream error text, never request content, never a URL, never a proxy endpoint.

Tests: the doctor units (`doctor_report_structure_matches_the_contract` — the whole
payload shape against the pinned structs; `engine_unreachable_degrades_honestly` — the
stopped/exited/faulted reasons and the transitioning case;
`engine_method_faces_answer_and_degrade_verbatim` — the answered-verbatim vs
failed-with-reason exclusivity; `lock_face_reports_absence` /
`lock_face_reports_a_live_held_lock` / `lock_face_reports_a_stale_dead_pid_lock` /
`lock_face_reports_a_released_lock_without_a_dead_pid` — the four classifications;
`recorded_pid_rejects_non_pid_content`; `probes_leave_the_data_directory_untouched` —
the read-only discipline by snapshot comparison;
`data_dir_face_reports_status_without_writing`;
`error_ring_is_bounded_and_aggregates_families`; `service_probe_classification_maps_every_literal`
— the five fixed literals from pure inputs plus a real `http::Response`, the
`not_configured` face included (no network anywhere);
`socks_layer_failures_carry_the_socks_marker_through_the_real_chain` — a loopback SOCKS refusal
classified through reqwest's real error chain;
`env_proxy_parser_tolerates_the_socks5h_prefix`); the integration roundtrip
(`tests/runtime_doctor.rs`:
`doctor_reports_versions_data_dir_lock_errors_and_reachability` — the whole payload over
a real authenticated socket with the fake engine live: the version chain verbatim, the
method faces riding the fixture's answers, the connector's own occupancy locks `held`
with the pid it wrote at acquire, real failed dispatches feeding the ring, and the
non-empty-params INVALID_REQUEST gate;
`doctor_service_face_answers_not_configured_without_proxy_env` — a connector spawned with
the proxy environment removed answers `proxyConfigured: false` and both probes
`not_configured` with no `status`/`latencyMs`, over the wire); plus the schema method
enumeration and the handshake advertisement.

### 4.80 Send-chain transport error classification: `SEND_RATE_LIMITED` / `SEND_PROOF_REQUIRED` / `SEND_SERVER_UNAVAILABLE` (contract revision 1.99, 2026-10-11)

The engine's send face folds transport-layer failures into the plain internal_error JSON-RPC
face (`code: -32603`) whose `message` embeds the cause class, so every rate limit, proof
challenge, and server outage reached the desktop as the generic retryable `UPSTREAM_ERROR`
family — indistinguishable from noise, and labeled retryable even though a retry cannot fix
a per-account rate limit. The connector now classifies the three fixed anchor literals the
engine contract pins verbatim (the JSON-RPC code stays -32603; only the classification
changes) at the single observation point that reads the raw upstream error body — the same
place §4.17's authorization classification lives:

- `(RateLimitExceeded)` — the upstream 429/413 rate-limit family → `SEND_RATE_LIMITED`
  (`retryable: false`)
- `(ProofRequiredException)` — the 428 proof-challenge family → `SEND_PROOF_REQUIRED`
  (`retryable: false`)
- `(ServerUnavailableException)` — the 5xx server-unavailable family →
  `SEND_SERVER_UNAVAILABLE` (`retryable: true`)

Contract faces:

- the `retryable` flag is display-side only: a mutating send is still never auto-retried by
  the connector, and an unknown send outcome keeps §6.4's never-resend posture. A per-send
  rate limit or proof challenge is an answer the caller re-drives; only server unavailability
  may succeed on a later attempt.
- the classification carries no event: a transport failure never touches account state, and
  no store row changes — the wire error is the whole surface.
- the authorization matcher keeps head-of-line precedence and its match set untouched: the
  §4.17 `AccountUnauthorized` → `ACCOUNT_UNLINKED` chain cannot regress. The transport
  classifier is a standalone pure function (`upstream_error_send_class`), deliberately not a
  widening of the fragile prefix/suffix authorization route — the anchors are parenthesized
  exception names embedded in the message text, so a substring match cannot pick up
  incidental content, and an error message carrying no anchor keeps the generic upstream
  answer byte-for-byte.
- the three codes enter the schema error-code enum; the structured-code allowlist ladder is
  untouched (the two cannot collide: an embedded anchor in a longer message vs. an exact
  allowlist literal).

Tests: the classifier units (`send_chain_classifier_maps_the_three_transport_anchors` — the
three anchors embedded in realistic engine messages plus the bare-literal shape;
`send_chain_classifier_answers_none_outside_the_anchor_contract` — the None ladder: no error
object, missing or non-string `message`, an anchor only in the `code` slot, near-miss
literals without the parenthesized exception name, ordinary engine errors, and the §4.17
authorization signatures;
`upstream_match_set_stays_clear_of_send_anchors` — the red line: the authorization matcher
answers false on all three anchor messages;
`send_chain_anchor_errors_classify_without_account_events` — the wire shape through the
engine line handler, each anchor to its variant, no event;
`authorization_classification_precedes_send_chain_anchors` — head-of-line precedence); the
service-layer mapping unit (`send_chain_transport_errors_map_to_send_error_codes` — the
three host codes with their contract-fixed retryability and no invented data object); and
the schema error-code enumeration (`tests/schema_consistency.rs`).

### 4.81 Engine D-batch consume faces: read/viewed sync, group changes, quote attachments, device capabilities (contract revision 1.100, 2026-10-11)

The engine's P8-D alignment batch (kt-signal-engine, 2026-10-11) landed four additive wire
faces the connector did not consume yet. This batch consumes them connector-side, wire
additive on both faces it adds (one event, one method), existing fields untouched.

#### D2 — cross-device read/viewed sync (`syncMessage.read` / `syncMessage.viewed`)

The engine projects the official `#handleRead`/`#handleViewed` faces as
`envelope.syncMessage.read[]` / `.viewed[]` entries `{senderUuid, senderNumber?, timestamp}`
— the (senderAci, timestamp) pair per marked message, senderNumber only when the engine's
contacts cache resolved it. The connector previously dropped the whole face on the plain
`syncMessage` skip. Consumption:

- **Parse bounds** (§4.13 family): per-face entry cap 256 (the receipt-batch guard,
  entries past it truncate); identity caps mirror the receive id family (128 chars); an
  entry without a non-empty `senderUuid` or a positive `timestamp` drops (宁缺勿错 — the
  engine already drops unparseable ones); an envelope with no usable entry keeps the plain
  skip. The control variant is `SyncReadState { read, viewed }`; the queue byte budget
  counts the entry lists explicitly (they are the one control variant with heap payload).
- **Routing**: intercepted before the conversation ladder like every own-account sync (the
  envelope source is our own account — the view-once open and messageRequestResponse
  precedents); each distinct sender resolves one direct conversation through the shared
  ladder — the entry ACI, the recorded ACI→peer-key mappings (§4.37), the entry's
  contacts-resolved number, contacts digit-suffix matches (§4.29). A sender naming no
  local conversation is warn-only dropped. Direct conversations only.
- **Row stamps**: each face resolves its own entries' timestamps to this conversation's
  incoming rows (upstream `sent_at` equality; caller-order, deduplicated, missing
  timestamps absent — never invented) and stamps its own column first-stamp-wins:
  `read_at` for read entries, `viewed_at` for viewed entries (the §4.77 stamp
  discipline). Outgoing and system rows never qualify — the entries name messages the
  account received.
- **Badge convergence**: the conversation badge is a coarse per-conversation counter with
  no per-row unread model (§4.29 — the desktop owns its read state; the counter exists
  for the badge), so any matched entry clears it through the same primitive the
  connector's own open path runs (`messages.list`), and the refreshed summary rides
  `conversation.changed` with the account total on `account.changed` (the insert-path
  precedent). Recorded deviation: an entry naming only an old row also clears the badge —
  the coarse-counter cost, preferred over inventing per-row unread state.
- **Events**: `message.statusChanged` per moved row (the row's own `status`,
  `delivered_at`, the stamp the face just wrote; absent keys stay absent — the 1.32
  absent-means-no-op merge discipline), then the badge pair. Replayed syncs stamp nothing
  and emit nothing.

Tests: the engine routing unit (`sync_read_viewed_routes_bounded_control_entries` — both
faces in one envelope, unusable entries drop, the routing source stays None, the
empty-face envelope keeps the plain skip); the store half
(`sync_read_store_half_resolves_timestamps_and_stamps_first_only` — caller-order
resolution with dedupe and misses, first-stamp-wins, incoming-only, the empty-list no-op);
the service unit (`sync_read_state_stamps_rows_and_converges_the_badge` — two faces two
rows, the badge pair rides a matched entry, the no-row timestamp stays silent, the replay
is silent, the unknown sender is silent).

#### D3 — group change batches (`groupChanged`)

The engine projects one decrypted group update batch carrying non-timer/terminate changes
as `envelope.groupChanged {groupId, revision, editor?, expireTimerSeconds?, changes[]}` —
the official GroupV2ChangeDetailType word list (title/avatar/description/access-\*/
member-\*/pending-\*/admin-approval-\*/member-add-from-\*/group-link-reset/
announcements-only), entries verbatim, the official-non-rendering classes never reaching
the wire. The connector previously dropped the whole face. Consumption — a pipe, not a
renderer (the desktop renders the change notices; the conversation row's group state
comes from the live `groups.get` face):

- **Parse bounds** (§4.13 family): the guaranteed faces fail closed — a missing or
  non-u64 `revision`, a missing or non-array `changes`, or an empty batch skips (a
  protocol error, not an unknown shape); `groupId` absence still routes so the service
  layer keeps its warn-only drop (the groupUpdated precedent); `editor` rides through the
  same bounded form that face reads (≤64 bytes, hostile shape reads absent); the
  `changes` array passes bounded and verbatim — 32-entry cap (the schema mirrors it,
  `GROUP_CHANGED_MAX_CHANGES`), entries past it truncate, material over the 64 KiB coarse
  guard drops the whole face (宁缺勿错, never a truncated lie). The timer leg parses
  beside the others (`u32` fail-closed; a non-u32 shape reads absent — the mirror skips,
  the event still forwards).
- **Routing**: the generic conversation ladder (`groupId` is the master key's standard
  base64 — the `listGroups` id form); an unknown group stays warn-only silent (the
  groupUpdated discipline — better to converge late than to invent state).
- **Convergence**: a batch that carried a timer leg converges the conversation timer
  through the §4.41 mirror exactly like `GroupTimerUpdate` (0 = off) — value-only, and
  the refreshed summary rides `conversation.changed` so a lagging window converges. No
  system row lands for a mixed batch: the official renders the whole batch as change
  notices, which the event carries — the timer-notice row stays the `groupUpdated`
  pure-timer face's.
- **Event**: `group.changed` (new) — `{accountId, conversationId, revision, changes,
  editor?, expireTimerSeconds?}`, the batch verbatim, absent-optional keys absent.
  Transient: nothing persists, a missed delivery is not self-healed.

Tests: the engine routing unit (`group_changed_routes_bounded_verbatim_changes` — the
batch rides verbatim, editor and timer leg parse, the three fail-closed skips); the
service unit (`group_changed_forwards_verbatim_and_mirrors_the_timer_leg` — the event
addresses the routed group, the batch rides verbatim, the timer leg converges the
conversation mirror and re-pushes the summary, the unknown group stays silent); the wire
frame (`group_changed_event_rides_the_batch_verbatim` — the verbatim batch plus the
absent-optional discipline); the schema event enumeration and the 32-entry bound pairing
(`tests/schema_consistency.rs`).

#### D4 — quoted-attachment metadata (`quote.attachments`)

The engine projects the official QuotedAttachment entries
(`{contentType?, filename?, thumbnail?}`) on the existing quote snapshot — the desktop
renders the quoted-media pill metadata-only. The connector previously carried
preview-only quotes, so a media quote rendered bare. Consumption — metadata beside the
preview, never bytes (§4.13):

- **Parse bounds** (§4.13 family): an 8-entry cap — entries past it truncate (the
  `MAX_INBOUND_ATTACHMENTS` discipline at quote scale); `contentType` rides the
  attachment-face cap (≤64 chars), `filename` the ≤128-byte one; the thumbnail rides
  the same bounded attachment parser the message face runs — an id-less pointer drops
  rather than rendering half a face, the entry survives on its own keys, and the
  locator half stays internal (`#[serde(skip)]`): a quote thumbnail rides the wire and
  the store as pure metadata, the honest 1.98 posture. An entry carrying none of the
  three keys says nothing and drops whole; key absence is protocol absence. The
  `filename` key keeps the engine's attachment-face spelling (the signal-cli wire
  key), not the official desktop's camelCase `fileName`.
- **Persistence**: the entries ride the quote snapshot JSON verbatim —
  `#[serde(default, skip_serializing_if = "Vec::is_empty")]`, so pre-1.100 rows
  deserialize with the empty face and an empty face serializes as an absent key
  (byte-identical to the old row shape, no migration); `messages.list` returns them
  verbatim.
- **Boundary**: the §4.38 history-import face keeps its quote snapshots metadata-only
  this revision — the backup NDJSON shapes its quote attachments differently and stays
  unparsed. The engine and sent-sync faces share the one parser.

Tests: the engine unit (`quote_attachments_ride_bounded_metadata_entries` — the
verbatim ride, the nothing-said drop, the id-less thumbnail drop, the cap truncation,
the sent-sync shared parser, the legacy-row deserialize); the service roundtrip
(`inbound_quote_and_attachment_metadata_roundtrip_through_the_store` — the quoted
metadata end-to-end through SQLite, the thumbnail's locator half staying internal).

#### D5 — device-capability registration read (`accounts.deviceCapabilities`)

The engine's KT-extension device face: the idempotent device-capability registration
(the official `registerCapabilities` endpoint, PUT — repeating the call repeats the
same server-side declaration, the official every-start semantics) with the upload
read-back the host can query. Consumption — a read face with one hard rule, the bare
identity never crosses the connector:

- **Routing**: the standard account ladder (`accounts.deviceCapabilities {accountId}`);
  `Lane::Read` with account-scoped mutating serialization (the `contacts.sync`
  precedent — the upstream face is a PUT), advertised through the method table; the
  engine call rides `CallClass::ReadOnly` (the declaration is idempotent, an
  indeterminate outcome carries no account-state risk).
- **Projection**: the engine answers `{account, registered, capabilities}` — the
  connector strips the `account` key and answers `{registered, capabilities}`; a bare
  identity never crosses the connector (the §4.13 metadata discipline at identity
  scale). The capability bits ride verbatim — the connector invents no keys and
  strips none (the official CapabilitiesUploadType key set, additive); the result
  face carries coarse bounds (16 entries, 4 KiB) that only a hostile or broken
  engine face trips, and an oversized face is a protocol defect, never a truncated
  lie. A missing or non-bool `registered`, a missing or non-object `capabilities`
  answers `UPSTREAM_PROTOCOL_ERROR`.
- **Errors**: the engine folds every failure — unknown/unregistered account, the 401
  unlink, any other server failure — into the plain `-32603`; the connector adds no
  matcher of its own and the face answers `UPSTREAM_ERROR`. The §4.17
  unlink-classification chain stays the engine-event worker's job; this face extends
  no anchor set (the generic call-site matcher applies as to every call, nothing
  more). No local persistence, no events.

Tests: the service unit (`device_capabilities_projection_strips_the_bare_identity` —
the bare-identity strip, the bits verbatim, the defect ladder, the coarse bounds, the
resolved-account prepare frame); the wire face
(`device_capabilities_strips_the_bare_identity` — the end-to-end answer without the
`account` key, the resolved-account upstream frame on the dispatch log, the
ACCOUNT_NOT_FOUND / INVALID_REQUEST ladder); the schema method enumeration and the
params `$def` (`tests/schema_consistency.rs`).

### 4.82 Credential-death classification and the doctor credentials face (contract revision 1.101, 2026-10-11)

The 2026-10-09 real-device debugging chain (CSS → DNS/socks5h → proxy split → the
credential 401) exposed the one layer the desktop cannot see: a dead linked-device
credential surfaces to the user as the generic retryable `UPSTREAM_ERROR` family —
"send failed, please retry" — because the credential-death evidence never survives
to a face the connector's matchers can classify. This batch closes the
connector-side half; the engine-side residual is recorded below.

#### The plain-text 401 anchor (the classifier)

The Signal chat service answers a dead credential with HTTP 401 whose body is the
exact plain-text sentence `Credentials are required to access this resource.` —
never a JSON error object, so the §4.17 matchers (exception names, the
`User … is not registered.` prefix/suffix pair) cannot see it. The connector
classifies the sentence as a standalone pure matcher
(`upstream_error_is_credential_invalid`) beside the frozen §4.17 match set — the
§4.80 boundary note applies verbatim — folding into the same
`EngineError::Unauthorized` arm at the same single observation point: the
credential is dead, the recovery is the same relink, and the
`AccountUnauthorized` → `ACCOUNT_UNLINKED` chain (retryable=false, the store's
`device_unlinked` state) is the independent credential-invalid state, distinct
from the retryable network-error families by construction. The match is one exact
case-sensitive sentence: case variants, partial sentences, the §4.17 signatures,
the §4.80 transport anchors, structured codes, and ordinary engine errors all
answer false — incidental text can never flip an account's state through this
matcher. No new host error code: `ACCOUNT_UNLINKED` is the face.

#### The doctor credentials face (the projection)

`runtime.doctor` gains a per-group `credentials` face — the doctor's one
account-authenticated probe, the layer the unauthenticated §4.79 service probes
cannot see (chat's 401 is the healthy edge answer for a probe with no
credentials, while a dead linked-device credential is only observable through an
authenticated call). One read-only `getUserStatus` ride through the group's own
live engine for the watchdog's any-account-of-this-group ping target, under the
doctor's 3-second diagnostic budget, folding into exactly one fixed literal:

- `healthy` — the engine answered the registered face.
- `credentialsInvalid` — the engine answered the credential-death face (the same
  classification the §4.17 unlink chain rides; a doctor probe that discovers it
  converges account state exactly like any other request would).
- `indeterminate` — the engine answered a different error: honestly
  unclassifiable at this boundary (see the engine residual below) rather than a
  guessed verdict.
- `unreachable` — no live engine to probe through (the engine face beside it
  carries the granular reason).
- `noAccount` — live engine, but the group has no account to probe.

The report carries only the literal — never the probe target (the account number
rides only into the engine call params), never error text, never a URL. The gate
is the engine face's reachability rule; the probe reuses the existing read-only
call path with the same budget, adds no thread and no polling, and a face that
cannot run degrades honestly while the call still succeeds.

#### The engine-side residual (recorded, owned by the engine batch)

The dominant real-world credential-death path is the websocket-upgrade 401: the
vendored `PushService::ws` upgrade goes through `reqwest-websocket`, whose
handshake failure drops the HTTP status (the 401 body included), so the error
folds to `ServiceError::WsError` — never `ServiceError::Unauthorized`. The
engine's fold points (`get_user_status`, the send-chain `service_error_face`)
match only `Unauthorized`, so a 401 at the upgrade layer answers the
content-free `Internal error` face: the connector sees the retryable generic
family (the desktop's "send failed, please retry") and the liveness watchdog
counts ping failures instead of an unlink. Until the engine classifies the
upgrade-layer 401 (e.g. reading the upgrade response's status before
`into_websocket`, or a REST whoami re-probe when the identified websocket
upgrade fails), the doctor's `credentials` face answers `indeterminate` on that
path — honest, and precise the moment the engine fix lands; the connector-side
anchor matcher above is the net for any fold point that carries the upstream
sentence. The fix belongs to the engine repository (read-only during this
batch); no connector-side change can recover a status the upgrade layer already
dropped.

Tests: the classifier units (`credentials_401_body_anchor_classifies_as_unauthorized`
— the bare sentence and an engine-wrapped shape through the real line handler,
each to `EngineError::Unauthorized` with the attributed `AccountUnauthorized`
event; `credentials_anchor_match_set_stays_narrow` — the None ladder, the
near-miss ladder, and the §4.17/§4.80 sets the matcher must not absorb); the
doctor units (`credentials_state_literal_maps_every_branch` — every fold from
pure inputs with no engine; `credentials_face_degrades_without_engine_or_account`
— the gate's skip branches and the face's account-free serialization; the
structure test pins the new face in the whole payload); the wire roundtrip
(`tests/runtime_doctor.rs`: `doctor_credentials_face_reports_credentials_invalid_on_auth_failure`
— the unlink signature planted, the group face answering `credentialsInvalid`
over the wire; the whole-payload test gains the `healthy` assertion). No schema
change: the doctor result stays a free-form object (the 1.97 precedent), the
method/error/event enums are untouched, and `tests/schema_consistency.rs` stays
green unchanged.

### 4.83 Delete-for-me sync: the message.deleteForMe event + the store mirror (contract revision 1.102, 2026-10-11)

The engine D-batch (§4.81) taught the connector to consume `syncMessage.read`
and `.viewed`; the same family carries one more projection the connector was
dropping on the plain skip: `syncMessage.deleteForMe` — the account deleted
messages **for itself** on another linked device, the official
#handleDeleteForMeSync face (Signal-Desktop `MessageReceiver.preload.ts` →
`deleteForMe.preload.ts` → `removeMessageById`, an outright row removal with
the conversation re-count, not a tombstone). One account deleting its own copy
is not the remote-delete face (§4.29's `mark_remote_deleted`): nothing is
revoked upstream, other devices and recipients keep their copies, and the
desktop must converge its local rows the same way. Three faces, in wire order:

**The receive.** The projection rides the envelope as
`syncMessage.deleteForMe = {messageDeletes: [{conversationId?, messages:
[{author, timestamp}]}]}` — `author` an ACI/PNI string or an e164 number,
`timestamp` the deleted message's sent stamp (the engine projects `0` for an
unstated one). The parse is the read-face discipline at delete scale, entirely
fail-closed: `normalized_sync_delete_groups` takes at most 64 groups
(`MAX_SYNC_DELETE_GROUPS`) and 256 entries per group
(`MAX_SYNC_DELETE_GROUP_MESSAGES`, the real batch scale — a full
per-conversation cleanup stays far below), trims and drops an empty author and
truncates any survivor at the receive-id bound (128 chars,
`MAX_RECEIVE_ID_CHARS`), drops an entry whose timestamp is not strictly
positive, drops a group whose entries all died, and answers empty for an
envelope with no usable group at all — the empty answer falls through to the
plain `syncMessage` skip (宁缺勿错: a partially-garbled batch deletes only
what it can prove, and a fully-garbled one never becomes a phantom event).
`control_routing` intercepts the surviving groups before that skip beside the
read/viewed arms — `direction: "incoming"`, `source: None`, `group_id: None`
(the envelope is our own account; the groups name their own conversations and
the entries their own authors, the read/viewed precedent) — and the queue
budget counts the control entry-wise like the read lists (`estimated_bytes`:
the group structs plus their heap strings and entries), so the §4.13 queue
stays honest.

**The store mirror.** The connector's store keeps row-level message state, so
the desktop's own (#handleDeleteForMeSync) first leg happens here: a resolved
(author, timestamp) pair against a resolved conversation removes the matched
rows outright — `delete_messages_by_sent_at` addresses rows by
(conversation, sent_at) alone and deletes **any direction** (the
`mark_remote_deleted` precedent; a same-stamp incoming/outgoing pair both die,
matching the official row deletion), takes at most 256 hits per entry
(the parse cap is also the delete cap), and repairs the touched conversation's
`last_message`/preview/unread summary and the account aggregates in the same
transaction (the `sweep_expired_messages`/`prune_history` store family, the
insert-path's inverse). A pair naming no local row deletes nothing and rides
silent — the mirror never invents rows or conversations (宁缺勿错). Each
conversation that actually lost a row emits `conversation.changed` with the
refreshed summary, and any removal at all closes with one `account.changed`
carrying the refreshed account total — exactly the `message.expired`
event-pair precedent. A replayed control finds its rows already gone: the
second mirror pass removes nothing, emits nothing, and converges (the
idempotency discipline).

**The forward.** The connector is not the only mirror — the desktop holds its
own rows and must remove them too. The groups forward as the new transient
`message.deleteForMe` host event: `{accountId, deletes:
[{conversationId?, messages: [{author, timestamp, messageId?}]}]}`, absent
optionals staying absent (the story-received discipline). The enrichment is
the connector's resolution work reused: a group whose conversation identity
resolves carries the local `conversationId` — the projection's own id, which
ladders as a 32-byte standard-base64 group master key (the `GROUPV2_ID_LENGTH`
oneof discipline) to a direct group-row address, or as a
service-id/e164 string through the §4.81 read-sync resolution
(`sync_sender_conversation`); absent that, when every entry's author laddered
to one and the same thread, that thread rides (the desktop gets a scoped
group); an author resolving nothing — the account's own sends have no peer
thread — rides unenriched and mirrors nothing, exactly the official
no-target-conversation group drop; authors laddering to different threads
leave the group conversation-less rather than mis-scoped. An entry whose pair
removed a local row carries that row's `messageId` (the first hit's id when a
sent-at stamp matched several rows — the desktop drops the handle directly and
its own (author, sentAt) join clears any stragglers). The event forwards even
when nothing resolved: the desktop joins its own rows by the (author,
timestamp) pairs alone. The event is transient beside the mirror — the
removed rows are already gone store-side, so a missed delivery converges on
the next conversation read exactly like `message.expired`; no replay (a replayed sync forwards the event but mirrors nothing and touches
no summary — the replayed-receipt idempotency discipline), no new method, error
code, or
capability tag (the `message.expired` precedent for core consume events).

Deliberate divergences, recorded honestly: (1) the mirror does not verify the
entry's `author` against the matched row's sender — the address is
(conversation, sent_at), the `mark_remote_deleted` precedent, so a row the
projection names with a stale author still dies; the official client behaves
the same way (its row deletion keys on the same pair). (2) Replayed syncs
forward the event but mirror nothing — the event is the desktop's own
idempotent remove, safe to double-deliver. (3) A same-stamp multi-row hit
reports only the first removed row's `messageId`; the wire shape is
entry-scoped and the desktop's own join covers the rest.

Tests: the engine units (`sync_delete_for_me_routes_bounded_groups` — the
bounded parse: valid groups route with source None, oversized groups and
entries truncate, zero timestamps and empty authors drop, an all-dead batch
falls back to the plain skip); the store units
(`delete_messages_by_sent_at_removes_any_direction_and_repairs_the_summary` —
the any-direction deletion, the 256-hit cap, the summary and account
repair); the service integration
(`sync_delete_for_me_forwards_enriched_and_removes_matched_rows` — the group
conversation resolution, the per-entry author ladder, the own-author drop,
the mirror's row removal with the summary/account event pair, the absent-optional
enrichment, and the replayed control forwarding without re-mirroring); the wire
frame (`delete_for_me_event_rides_the_resolved_groups` — the event name and the
camelCase shape); and the schema consistency pairs (the 64/256/128 bounds pinned
to `MAX_SYNC_DELETE_GROUPS`/`MAX_SYNC_DELETE_GROUP_MESSAGES`/
`MAX_RECEIVE_ID_CHARS`, the event enum auto-balanced against the emission
literal). Gates: 614 → 618, all green.

### 4.84 Verified trust sync: the identity.verifiedChanged event (contract revision 1.103, 2026-10-11)

The engine B4 identity surface projects one more sync face the connector was
dropping on the plain skip: `syncMessage.verified` — the account's own linked
device settled a peer's safety-number trust state, the official
SyncMessage.verified face. The engine's multi-device trust convergence already
did its authoritative work before this projection ever rides the wire:
`identity::apply_synced_trust` writes the sidecar trust store, then the
projection notifies the downstream connector. The official client consumes the
same face by flipping its conversation's verified presentation
(DEFAULT/VERIFIED/UNVERIFIED), so the desktop's need is a notify it can join
against its own conversations — not a mirror the connector must reconcile.

**The receive.** The projection rides the envelope as
`syncMessage.verified = {destinationUuid, destinationNumber?, identityKey?,
state}` — `destinationUuid` the peer's ServiceId UUID string (the engine
projects `service_id_string()`, never a number here), `state` one of the
engine's lowercase trust words (`verified` / `unverified` / `default`, the
`TrustState::word()` output), the number the e164 the engine's contacts cache
resolved, the key the hex identity key the proto stated (66 chars on the
modern EC key). The parse is fail-closed at two hard gates: the state word
must be exactly one of the three whitelist members (a non-member string or
any non-string shape is a hostile face — the wire enum numbers the proto
carries never reappear downstream), and the destination must survive the
ServiceId UUID shape gate verbatim (the `messageRequestResponse` thread-leg
gate). Both hard gates failing drops the whole record and the envelope falls
back to the plain `syncMessage` skip — a verified sync that cannot prove its
subject is not an event (宁缺勿错). The optional keys fail per-key: a wrong
JSON type, an empty survivor, or an over-bound string answers absent — never
a truncated lie (an identity key is a fingerprint; shortening one mints a
wrong safety number; the key bound `MAX_SYNC_IDENTITY_KEY_CHARS` = 128 only
ever bites on a hostile face). The queue budget counts the record's heap
strings (§4.13), and `control_routing` intercepts the surviving record before
the plain skip beside its read/viewed/delete-for-me siblings —
`direction: "incoming"`, `source: None`, `group_id: None` (the envelope is
our own account; the record names its own destination).

**The store mirror — deliberately none.** Three honest counts, recorded as
the discretion §4.84 was asked to make: (1) the authoritative write already
happened upstream — the engine applied the state to its sidecar store before
projecting, so a connector mirror would be a copy of a copy; (2) the
connector store has no trust bit to hang the state on — the `conversations`
row carries thread state (peer_key/title/unread/muted/pinned/timer/mrr
factors), the `contacts` row carries directory material, the
`peer_identities` table carries the ACI→peer-key mapping, and none carries a
trust column; inventing one would create a second authority that drifts from
the engine's store with no reader; and (3) the host's own reconciliation path
already exists — the §4.61 `identities.get` read-through projects the
authoritative upstream trust face (`trustLevel === 'TRUSTED_VERIFIED'` is the
desktop's own verified derivation), so the event's job is only to say *when*
to re-read, not to carry the truth itself.

**The forward.** The record forwards as the transient
`identity.verifiedChanged` event: `{accountId, destinationUuid, state,
destinationNumber?, identityKey?, conversationId?}` — flat, absent optionals
staying absent (the story.received conditional-insert style). The one
enrichment is the §4.81 resolution family: the destination walks its own
number first (when the projection resolved one) then the ACI rungs (the raw
UUID, the recorded ACI→peer-key mappings, the contacts digit-suffix
matches); a hit carries the local `conversationId` (the id form the desktop
addresses), a miss stays conversation-less and the record still forwards —
the trust state is self-describing by its destination, so the host joins by
`destinationUuid` alone when the connector held no thread. The event is
transient: nothing persists, nothing self-heals store-side; a missed delivery
converges through the next `identities.get` read exactly as the official
client's own presentation does. No new method, error code, or capability tag
(the `message.expired` precedent for core consume events).

Tests: the engine unit (`sync_verified_routes_the_whitelisted_state` — the
whitelist and UUID gates, the per-key fail-closed optionals, the plain-skip
fall-through); the service integration
(`sync_verified_forwards_with_the_resolved_thread` — the ladder enrichment,
the conversation-less unknown destination, the single-event face with no
summary convergence); the wire frame
(`verified_changed_event_rides_the_flat_record` — the event name, the flat
camelCase shape, the absent optionals); and the schema consistency pairs (the
destination/key maxLengths pinned to `MAX_RECEIVE_ID_CHARS`/
`MAX_SYNC_IDENTITY_KEY_CHARS`, the event enum auto-balanced against the
emission literal). Gates: 618 → 621, all green.

### 4.85 Call-history sync: the call.historyEvent event (contract revision 1.104, 2026-10-11)

The 2026-10-06 engine batch taught the connector's engine to project one more
sync face the connector was dropping on the plain skip: `syncMessage.callEvent`
— the account's own linked device wrote one 1:1/group call's history entry,
the official callEventSync face. The official consumer (`onCallEventSync` →
`updateCallHistoryFromRemoteEvent`) resolves the record's conversation, drops
the record when the lookup misses, and writes its **own** call-history store —
so the connector's job is a bounded, honest pipe, not a mirror: the desktop's
call log is the authoritative store, and duplicating it connector-side would
fork the truth with no second reader.

**The receive.** The projection rides the envelope as
`syncMessage.callEvent = {callId, timestamp, type?, direction?, event?,
conversationId?}` — the call id the decimal string the official
`longToString` form carries (the engine projects `u64::to_string()`), the
record timestamp (the engine projects 0 for an unstated one), the
type/direction/event words the engine's enum projections resolve to lowercase
(`audio|video|group|ad-hoc|unknown`, `outgoing|incoming|unknown`,
`accepted|not-accepted|delete|observed|unknown`), and the conversation
identity in the official three-form shape (hex room id for ad-hoc, ServiceId
UUID string for a 16-byte direct conversation, standard base64 group master
key otherwise). The parse is fail-closed at two hard gates on the record's
primary key: the callId must be a non-empty all-ASCII-digit string at the u64
ceiling (`MAX_SYNC_CALL_ID_CHARS` = 20 — anything non-numeric, over-bound, or
non-string is not the record's key), and the timestamp must be strictly
positive (the read-face unknown-marker discipline — a record with no
timestamp is not a history record). Either gate failing drops the whole face
and the envelope falls back to the plain `syncMessage` skip — a call record
that cannot prove its key is not an event (宁缺勿错). The word keys fail
per-key: a non-member string or any non-string shape answers absent — the
desktop renders its own word for a missing leg, never a mangled one — and the
conversation identity rides verbatim in whatever form the projection chose
(the connector never re-encodes; the ladder reads the form). The queue budget
counts the record's heap strings (§4.13), and `control_routing` intercepts
the surviving record before the plain skip beside its verified sibling —
`direction: "incoming"`, `source: None`, `group_id: None` (the envelope is
our own account; the record names its own conversation).

**The store mirror — deliberately none.** The connector keeps no call-history
store: the schema has no call table, no call columns, no call state anywhere.
The task discipline says exactly this: inventing call-history storage here
would fork the desktop's authoritative log (the official consumer is the
history writer), and the connector cannot replay what it never held — so the
record is transient, and a missed delivery loses that one entry exactly as
the official client loses one it received while offline. No self-heal, no
replay, no summary convergence beside the event.

**The forward.** The record forwards as the transient `call.historyEvent`
event: `{accountId, callId, timestamp, type?, direction?, event?,
conversationId?}` — flat, absent optionals staying absent (the
story.received conditional-insert style). The one enrichment is the §4.85
conversation ladder, the §4.83 delete-for-me group ladder verbatim: the
32-byte standard-base64 form (a group master key) addresses its group row
directly (kind-gated to a group row); everything else — the 16-byte UUID
form, an e164, an ad-hoc hex room id — walks the §4.81 read-sync resolution
(the raw value as the number/ACI rung, the recorded ACI→peer-key mappings,
the contacts digit-suffix matches); the hex room id naturally resolves
nothing (the connector keeps no ad-hoc face) and stays honestly
conversation-less. A hit carries the local `conversationId` (the id form the
desktop addresses), a miss forwards conversation-less — the desktop joins by
the projection's own identity, the same lookup the official consumer runs.
No new method, error code, or capability tag (the `message.expired` precedent
for core consume events).

Tests: the engine unit (`sync_call_event_routes_the_shape_gated_record` —
the callId/timestamp gates, the per-key word whitelists, the verbatim
conversation identity, the plain-skip fall-through); the service integration
(`sync_call_event_forwards_with_the_resolved_conversation` — the 32-byte
group direct address, the UUID ladder, the ad-hoc hex miss, the
conversation-less no-identity face, the one-event-no-convergence count); the
wire frame (`call_history_event_rides_the_flat_record` — the event name, the
flat camelCase shape, the absent optionals); and the schema consistency pair
(the callId maxLength pinned to `MAX_SYNC_CALL_ID_CHARS` — the conversation
identity rides the shared `opaqueId` def, no inline bound to pin, the event
enum auto-balanced against the emission
literal). Gates: 621 → 624, all green.

### 4.86 Edit-send migration: `messages.edit` rides the engine `editMessage` face (contract revision 1.105, 2026-10-11)

The audit's one ❌-class send-path break closes here. `messages.edit` was the last surface
still speaking the signal-cli era: the supervisor dispatched the upstream `send` method with
an `editTimestamp` parameter. The kt-engine `send` carries no `editTimestamp` parameter — the
engine's parameter reader ignored it — so every desktop edit crossed the wire as a
brand-new message (a duplicate landed on the remote; the original stayed unedited; the
engine's `editMessage` method, wired in its 2026-10-11 six-questions batch for the official
edit send face, sat unused behind the connector).

The migration is connector-internal; the host wire face does not move:

- **Host face unchanged** — `messages.edit` keeps its params
  (`accountId`/`conversationId`/`messageId`/`text`/`clientRequestId`), its message-addressed
  row resolution (§4.14: only an outgoing row in the terminal `sent`/`delivered`/`read`
  state carries the upstream protocol identity), its `MessageRecord` settlement result
  (body replaced, `editedAt` set, status untouched), and its indeterminate discipline
  (`SEND_OUTCOME_UNKNOWN`, row unchanged, never auto-retried). Zero desktop changes.
- **Upstream params** — `prepare_edit` emits `{account, message, targetTimestamp}` plus the
  `set_upstream_target` addressing: `targetTimestamp` is the engine's u64 key for the edited
  row's protocol timestamp (the same key name `remoteDelete` dispatches on; the old
  `editTimestamp` key was the signal-cli dash-dest transliteration and no engine code reads
  it). `message` is the non-empty validated body the engine's dispatch gates on (the
  official `canEditMessage` body gate); the connector's 1..65536-byte `validate_text` bound
  already covers it.
- **Addressing legs** — the engine's `editMessage` parses exactly-one addressing leg
  (sendMessageRequestResponse 同构): the direct leg through `recipient` (string array,
  non-empty) and the group leg through `groupId` (32-byte group master key, standard
  base64). `set_upstream_target` has produced exactly that pair for `send`/`sendTyping`/
  `remoteDelete`/`sendReaction` since 1.10; no shape change was needed. Both legs present,
  both missing, or either malformed answer the engine's `-32603`, which surfaces through the
  existing §4.17 error ladder as `UPSTREAM_ERROR` — the same surface a late edit's official
  24 h window rejection rides (the window stays an upstream/UI gate, §4.14 unchanged).
- **Settlement unchanged** — the engine answers `{timestamp, results}` (the `send` result
  shape); the supervisor's confirmed-success path is value-blind, so the settlement row
  rewrite (`apply_outgoing_edit`) and the `message.upserted`/`conversation.changed` event
  flow are byte-identical.

No new method, error code, or capability tag; no queue/store shape moves — the only wire-surface
text change is the `messages.edit` schema description (no shape delta, additive-only revision
discipline). The receive-side edit paths (§4.13 inbound control plane, the envelope-level
`editMessage` normalization) were already engine-native and are untouched.

Tests: the service unit `prepare_edit_targets_sent_row_and_settlement_rewrites_it` asserts
the `targetTimestamp` key on the prepared params (the one assertion the migration moves);
the host/registry/lane surfaces are covered by their existing tables (`messages.edit` keeps
its `messages.edit` row: send lane, account-scoped mutating, `send` metrics class). Gates:
624 → 624, all green.

### 4.87 Sync seven-arm consume: the plain-skip batch speaks its projections (contract revision 1.106, 2026-10-11)

The audit's receive-side gap closes arm-for-arm. Seven engine sync projections —
`syncMessage.configuration` / `fetchLatest` / `callLinkUpdate` / `callLogEvent` /
`deviceNameChange` / `usernameChange` / `pniChangeNumber` — all fell to the plain
`syncMessage` skip: the envelopes arrived, the connector logged the skip, and the faces the
official client acts on (`MessageReceiver.preload.ts`'s `#handleConfiguration` through
`#handlePNIChangeNumber`) never reached the desktop. Each arm now follows the §4.84/§4.85
paradigm exactly: one fail-closed engine parse, one `control_routing` interception before the
plain skip, one transient host event. All seven ride the account's own sync envelope — every
arm routes with `direction: "incoming"`, `source: None`, `group_id: None` — and the queue
budget charges each arm its fixed struct width (the §4.13 discipline; the two bare-signal
triggers charge nothing).

**The receive — seven arms, each fail-closed at its own facts.**

- `configuration` (`settings.configurationSync`) — the official
  SyncMessage.Configuration flags, projected key-for-key:
  `{readReceipts?, unidentifiedDeliveryIndicators?, typingIndicators?, linkPreviews?}`.
  The read is per-key (a non-bool shape answers absent — a mangled flag is never
  forwarded); a face where no key survived is a zero-content sync and keeps the plain
  skip.
- `fetchLatest` (`sync.fetchLatest`) — the official handleFetchLatest trigger routes on
  presence alone: the official fires FetchLatestEvent even with the type unset (UNKNOWN
  is the downstream no-op), so this face hard-gates nothing. The optional
  `{type?}` word rides the engine's SCREAMING enum whitelist per-key
  (`UNKNOWN` / `LOCAL_PROFILE` / `STORAGE_MANIFEST` / `SUBSCRIPTION_STATUS`; a hostile
  word answers absent, never mangled).
- `callLinkUpdate` (`call.linkUpdate`) — two hard gates, exactly the facts the official
  handler throws over: the type must be present and the projection's only known member
  `UPDATE` (a null or unknown word drops the record), and the `rootKey` must be present,
  non-empty, within `MAX_SYNC_CALL_LINK_KEY_CHARS` = 128 — an over-bound key drops the
  record whole, never truncated (a shortened key would address a different call link).
  The `adminKey` is per-key fail-closed. Both keys are material, never parsed: the base64
  rides verbatim (the §4.73 call-link zero-parsing discipline; the connector runs no call
  link, so there is nothing to verify the material against).
- `callLogEvent` (`call.logEvent`) — one hard gate on the record's whole meaning: the
  type verb must be a whitelist member (`CLEAR` / `MARKED_AS_READ` /
  `MARKED_AS_READ_IN_CONVERSATION` / `CLEAR_IN_CONVERSATION` — the engine's SCREAMING
  projections; the official zod enum parse fails the same face, and an absent or hostile
  verb drops the record whole). The remaining keys are per-key optionals exactly as the
  official loose parse carries them: the `timestamp` and `callId` ride the engine's u64
  numbers verbatim, and the conversation identity rides the §4.85 three-form input
  (hex room id / ServiceId UUID / group master key base64) un-re-encoded.
- `deviceNameChange` (`account.deviceNameChanged`) — one hard gate: the `deviceId` must
  be present in u32 range (the official handler warns and drops the sync when the key is
  absent — nothing to address; the parse's `try_from` is the same bound). The id forwards
  verbatim; the local-device comparison stays the desktop's own decision.
- `usernameChange` (`account.usernameChanged`) — the official handleUsernameChangeSync
  face, an empty pure-signal sync whose presence alone is the event (the new value
  converges through the host's own profile read; the projection carries no payload and
  the connector invents none). A non-object shape (`"renamed"`, a number) is not the
  face and keeps the plain skip.
- `pniChangeNumber` (`identity.pniChanged`) — the engine projects only the stripped
  `{registrationId?, newE164?}` face — the change's key material never crosses the
  engine boundary and the connector never sees it. Per-key fail-closed (the
  registration id through the same u32 `try_from` bound); a face where neither key
  survived is the official's own empty-sync warning shape and keeps the plain skip.

**The store mirror — deliberately none, per arm.** The five account-level arms
(configuration / fetchLatest / deviceName / username / pni) are trigger semantics: they
name an event about the account, not a row in any store, and there is nothing to mirror.
The two data-plane arms keep no store the official consumer does not already own: the
connector holds no call-history store (the §4.85 discretion verbatim — the desktop's log
is the authoritative writer) and no call-link store (the §4.73 passthrough discipline —
the material is consumed by the official call-link stack, never by connector state).
Every record is transient: a missed delivery loses exactly what an official client loses
offline, and no self-heal, replay, or summary convergence rides beside the events.

**The forward.** Seven new flat host events (the story.received conditional-insert
style — absent optionals staying absent, no nulls invented):

- `settings.configurationSync` `{accountId, readReceipts?, unidentifiedDeliveryIndicators?,
  typingIndicators?, linkPreviews?}` — at least one flag present (the parse guarantees it).
- `sync.fetchLatest` `{accountId, type?}` — the SCREAMING word verbatim when carried.
- `call.linkUpdate` `{accountId, type: "UPDATE", rootKey, adminKey?}` — the record's
  subject always present (the parse guarantees it).
- `call.logEvent` `{accountId, type, timestamp?, peerId?, callId?, conversationId?}` —
  the projection's raw identity rides `peerId` (the official CallLogEventDetails name the
  desktop's join reads) while the §4.85 ladder's hit rides `conversationId`: across the
  whole event surface that key always means the resolved local conversation id, so the
  desktop can take the shortcut or run its own join over `peerId`.
- `account.deviceNameChanged` `{accountId, deviceId}`.
- `account.usernameChanged` `{accountId}` — the fact alone.
- `identity.pniChanged` `{accountId, registrationId?, newE164?}` — at least one key
  present; no key material ever rides the event either.

No new method, error code, or capability tag (the §4.84/§4.85 precedent for core consume
events); the schema gains the seven event enum entries, seven allOf data blocks, and four
pairing bounds inventoried below.

Tests: seven engine units (`sync_configuration_routes_the_flag_face`,
`sync_fetch_latest_routes_on_presence`, `sync_call_link_update_routes_the_root_keyed_record`,
`sync_call_log_event_routes_the_gated_record`,
`sync_device_name_change_routes_the_device_id`,
`sync_username_change_routes_the_bare_signal`,
`sync_pni_change_number_routes_the_stripped_projection` — each arm's gates, whitelists,
and plain-skip fall-through); two service integrations
(`sync_account_level_arms_forward_transient_events`,
`sync_data_plane_arms_forward_with_their_own_identities` — the five trigger forwards with
zero store delta, the two data-plane forwards each resolved against its own identity, the
one-event-no-convergence counts); two wire frames
(`account_level_sync_events_ride_flat`, `data_plane_sync_events_ride_flat` — the event
names, the flat camelCase shapes, the absent optionals); and four schema-pairing pins (the
call-link `rootKey`/`adminKey` maxLengths ↔ `MAX_SYNC_CALL_LINK_KEY_CHARS`, the device/PNI
registration-id maxima ↔ the parse's u32 `try_from` range gate). Gates: 624 → 635, all green.

### 4.88 Ingest key-list completion: the P14 live-projection faces ride the descriptors (contract revision 1.107, 2026-10-11)

The audit's ingest-side gap closes key-for-key. The engine's P14 live-projection batch
taught its receive face to project five families of message metadata — attachment
`caption` / `blurHash` / `incrementalMac` + `chunkSize` / `isBorderless` / `isGif`, the
link-preview `date`, the dataMessage `expireTimerVersion`, the typing `groupId`, and the
Sent transcript's three optional faces — and the connector's ingest key lists dropped every
one of them: the keys arrived on the envelope, the bounded per-key parsers answered
absent, and the row (and the wire record it feeds) carried none of them. This batch
completes the lists; nothing else moves.

**The attachment descriptor.** `normalized_attachment` (the §4.13 metadata-only parse,
shared by message attachments, preview images, quote thumbnails and stickers) now carries
the six P14 keys, each fail-closed at the engine's own projection shape: the `caption`
rides as text material under the inbound-text budget (empty = absent — zero information
projects nothing), the `blurHash` rides verbatim within a 128-char bound (never decoded —
the desktop renders its own progressive placeholder), and the streaming-integrity pair
`incrementalMac` + `chunkSize` survives only all-or-nothing (the official
`filePointers.preload.ts:84-87` discipline the engine already projects: half a pair
verifies nothing, so a half pair drops whole — never forwarded mangled). The two flag
booleans answer false on any non-bool shape and serialize even when false — the
`isVoiceNote` trio shape, so all three booleans read without key probing; the official
`isGIF`'s additional `contentType video/*` gate stays the desktop's judgement (the engine
projects the flag bit, the media class is the consumer's call — 宁多投不误判). The
archive import face (`import_attachments`, §4.38) parses the same-named keys and the same
flag bits (`Borderless` = 2, `Gif` = 8 — the enum members the live pointer projects), so
an imported row and a live row of the same attachment share one descriptor wire form — the
§4.38 test comment's own stated posture, now true in code.

**The rich faces.** `normalized_rich` gains `expireTimerVersion` (the dataMessage's
disappearing-message protocol stamp; positive, u32-bounded — the engine projects no key
for 0, and the timer value itself remains the row's `expireSeconds` face) and the
preview-card `date` (positive only — the official falsy posture verbatim). The three Sent
transcript faces parse on the same function because the sent mirror runs it on
`syncMessage.sentMessage`: `isRecipientUpdate` is true only when the proto stated true,
`unidentifiedStatus[]` carries each destination's service-id string (the PNI-prefixed
form for a PNI destination, the plain UUID for an ACI), its `unidentified` bit and its
`destinationPniIdentityKey` (standard base64 within the identity-key bound) exactly as the
engine resolved them (binary first, string fallback), and `storyMessageRecipients[]`
carries the destination, the `distributionListIds` verbatim (an empty list is the
official's list-narrowed semantics and stays an empty list) and `isAllowedToReply` only
when stated. Receive dataMessages never carry the three keys, so inbound rows keep their
defaults; every identity string takes the receive-id family bound and the outer lists the
sync receive family's 256-entry cap — a hostile face degrades per-key, never the row.

**The verified zero-delta.** The engine's typing `groupId` projection needed nothing: the
connector's typing arm has routed the group leg through the shared conversation resolution
since contract 1.15 — the projection and the consumption met years apart, and the audit
item closes as verified-consumed, not newly wired. One discipline stays untouched
(recorded): a bodyless, attachment-less dataMessage carrying only preview/P14 material
keeps the bodyless drop — the key-list completion extends what rides a landing row, never
which faces land one (键位扩齐不改行落位).

**The wire and the store.** No new method, event, error, or capability: the keys ride the
existing `message.upserted` record (serde — descriptor and rich serialize onto it
directly), the row's `attachments_json`/`rich_json` columns, and every read shape. The
old-row discipline holds two ways: pre-1.107 rows deserialize through `serde(default)`
(no key → default) and their optional keys re-serialize absent — byte-identical reads —
while the boolean trio serializes explicit `false`, the always-boolean shape every
descriptor has carried since `isVoiceNote`. The schema gains one additive description
paragraph on the read shape (no structural delta — the record face is prose-described by
design).

Tests: three engine units (`attachment_p14_metadata_keys_parse_per_key` — the full face,
the half-pair drops, the zero-information and hostile shapes, the legacy byte-identity;
`rich_p14_faces_parse_per_key` — the version/date/bitmap gates, the two transcript lists
with their bounds and per-key degradation, the plain-row collapse;
`legacy_attachment_and_rich_json_round_trip_without_p14_keys` — the old-row serde
discipline both directions); two service integrations
(`ingest_receive_p14_metadata_rides_row_and_event` — descriptor and rich faces identical
on the row and on the emitted `message.upserted` record;
`sent_mirror_transcript_faces_persist_on_the_mirror_row` — the three transcript faces
verbatim onto the mirror row). Gates: 635 → 640, all green.

### 4.89 Read/viewed SELF-SYNC upstream legs: `messages.syncRead` / `messages.syncViewed` (contract revision 1.108, 2026-10-11)

The audit's N0 finding, closed: the connector never spoke the read-state SELF-SYNC
channel. `messages.markRead`/`messages.markViewed` (§4.29) spend their fan-out on the
engine's `sendReadReceipt`/`sendViewedReceipt` — the PEER-facing ReceiptMessage channel,
gated by the account's `readReceipts` configuration flag (default off) — so the account's
own other linked devices never learned what this desktop had read. The engine's
`markRead`/`markViewed` faces (engine `methods.rs` P14 group 6, the official
`syncReadMessages`/`syncView` isomorphs, SendMessage.preload.ts:2097/:2145 — the
SyncMessage.read=5/viewed=16 self-send to the account's own ACI) sat unconsumed.

Two new wire methods close the second channel. `messages.syncRead {accountId,
conversationId, messageIds?}` and `messages.syncViewed {...}` carry byte-identical
addressing to the markRead pair (one wire semantic per method, the 1.34 pair precedent —
a `mode` parameter would conflate two protocol channels behind one name and complicate
the schema's per-method if/then pinning), and the prepared selection is byte-identical
to `prepare_receipt`: incoming rows only, both forms bounded at 512 (truncation, not
rejection), ascending `sentAt` with same-timestamp collapse, direct-conversation author
resolution only (group rows persist a local sender hash, §4.28 — their authors stay
unresolvable and they skip from the fan-out, the same documented boundary the peer face
carries; the §4.119 selection filters ride along — one shared store path). The supervisor
spends the group on ONE engine `markRead`/`markViewed` call —
`{account, reads|viewed: [{sender, timestamp}...]}`, the author inside each entry, the
engine chunking 100 entries per envelope internally — where the peer face made one call
per author. Receipt-class semantics end to end: `{status: sent|unknown}`, any failure
degrades and nothing retries (AGENTS.md); zero eligible rows is the trivial `sent`
no-op. `messages.syncViewed` keeps the §4.77 local-first discipline (the selected rows
take their first `viewed_at` via the shared `complete_mark_viewed` before the sync);
`messages.syncRead` stays stamp-free (the desktop owns its read state).

Why the channels stay separate methods instead of widening `messages.markRead`: the
desktop's conversation-open path should drive the self-sync leg on every open while the
peer receipt leg stays flag-gated best-effort — one method per channel lets the desktop
pick the channel by name and lets this contract describe each channel's gate, engine
face, and degradation independently. The `send-receipts` capability and the send lane /
account-scoped mutating classification carry over verbatim (the same write-lane mutex
and delete drain barrier serialize the sync faces against sends on the same account).
Schema: two method enum entries, two if/then pairs, two additive `$defs` param objects.
The desktop call-site migration is a separate batch (this batch is the connector's
semantic split only).

— the syncRead/syncViewed pair, the supervisor spending the group on one engine call,
receipt-class semantics end to end. Tests: one service integration
(`sync_read_and_sync_viewed_prepare_mirror_the_receipt_faces` — the peer group mirrors the
receipt face with ascending timestamps, syncViewed stamps locally with first-stamp-wins,
syncRead never stamps, group conversations skip the fan-out but keep the viewed stamp).
Gates: 640 → 641, all green. (Count bracket: this total predates the 1.116 recount —
the integration legs were over-counted by four in the earlier bookkeeping; the
reproducible all-targets base re-measured at 1.115 is 654. The per-batch delta of one
and the lib count were exact throughout.)

### 4.90 remoteDelete author and age gates (contract revision 1.109, 2026-10-11)

The inbound remoteDelete control was the one message-addressed arm that applied
without checking who was speaking or when the target was composed: the store's
delete matched (account, conversation, sent_at) alone. Two real defects fall out
of that shape. (1) The author defect is a live misattribution: Signal
timestamps are per-sender clocks, so in a group two members' rows can carry the
same `sent_at`, and any member's remoteDelete wiped every row sitting at that
timestamp — author A's delete deleted author B's message. (2) The age defect is
a hardening gap: the official delete-for-everyone window is 24 h at compose
time (docs/remote-delete-l2-plan.md §3.2), and the connector applied deletes
naming targets no honest sender could still have composed a delete for.

**The author gate (the store face).** `mark_remote_deleted` takes the delete
envelope's own author projection — the same derivation the landing row stored
as its `sender_id` (`stable_hash_id([account, kind, source])` for peers, the
account id itself for the outgoing multi-device mirrors) — and scopes both the
UPDATE and its read-back with `sender_id = ?`. Only the author's own row ever
transitions; an author with no row at the timestamp answers `None` (which the
§4.91 early-arrival buffer reads as the early-arrival question), a replay
still answers the already-deleted row with `changed=false`, and the direct-
conversation identity pair collapses to one string (source = peer key), so
existing direct deletes are byte-identical in behavior. The delete-for-me sync
keeps its deliberately author-less store path (`delete_messages_by_sent_at`,
§4.83): that face is the account's own self-sync and names its conversations,
not its authors.

**The age gate (the arm face).** The window reads the delete envelope's own
sender clock — the delta between the envelope `timestamp` and the named
target timestamp — not the connector's clock. Both values are the author's
assertions, so the delta is the compose-time age the official window governs
and the check is immune to sender/receiver clock skew; a fresh envelope naming
an ancient target (relay lag, stale replay) still trips it, while a
legitimately-composed delete that arrives late applies. A delete past the
`REMOTE_DELETE_MAX_AGE_MS` window (24 h) is dropped silently: no event, no
state change, a kind-only warn log. The gate is deliberately narrow hardening,
not a protocol re-derivation: a hostile author can forge both fields, so the
window exists to filter broken/stale faces (a corrupted or replayed timestamp
colliding with a real row), and it sits BEFORE the store call so an aged
delete can never consume the §4.91 buffer either (an aged delete only grows
older while it waits).

No wire change: the method/event/error enums are untouched, no new
capability, and `tests/schema_consistency.rs` stays green unchanged.

Tests: the store unit (`remote_delete_scopes_to_the_delete_author` — the
shared-timestamp group collision, the foreign-author None, the own-author
apply, the changed=false replay); the service arm units
(`group_remote_delete_is_author_scoped` — the live arm through the full
ingest ladder with the replay and the foreign-author silent faces;
`aged_remote_delete_is_dropped_silently` — one millisecond past the window
drops, one millisecond inside applies, the boundary is the window itself);
the two pre-existing summary tests now pin the author-scoped call shape.
Gates: 645 → 648 (lib 486 → 489), all green.

### 4.91 Early-arrival edit/remoteDelete buffer (contract revision 1.110, 2026-10-11)

The control plane's contract-1.15 discipline is "control receives never
create a conversation, never create a message row" — so an edit or a
remoteDelete whose original row (and sometimes the whole conversation) has
not landed yet had exactly one fate: a silent drop. On a cold start the
engine's receives race each other, and Signal delivers the control envelopes
on the same per-sender stream as the originals, so the race is real: the
edit/delete lands first, the drop eats it, and the row converges later with
yesterday's body — or stays standing that the author deleted. The §4.90
author gate made the drop deterministic (only the author's own absence is
the early-arrival question); this batch makes the absence recoverable.

**The buffer (the admit face).** One bounded flat table per connector
service, `EARLY_CONTROL_BUFFER_CAPACITY = 512` entries with a
`EARLY_CONTROL_BUFFER_TTL_MS = 5 * 60 * 1_000` millisecond TTL — the
256-entry typing limiter's flat-map discipline scaled to the batch shape:
every touch first sweeps expired entries (lazy, no timer), then a full
table evicts the single oldest entry, so the table never holds more than
the capacity and every entry dies at the TTL. Only two control families
admit: `RemoteDelete` (payload-free) and `Edit` with a body (the direction
and the verbatim text ride the entry; a bodyless edit keeps the historical
silent skip — nothing to apply later). Every other family, and a control
receive whose conversation resolves, never sees the buffer: the live arms
are byte-identical to the pre-§4.91 behavior.

**The key (the identity face).** An entry is keyed to the identity the
original row will carry when it lands: `(account, kind, peer_key,
target_sent_at, author_sender_id)` — the conversation resolves through the
same ladder `ensure_conversation` uses (the conversation row itself may not
exist yet), and the author projection is the exact `sender_id` derivation
§4.90 pinned (the account id for the outgoing multi-device mirrors, the
envelope source's peer hash for peers). The author component doubles as the
flush-time gate: an entry only ever applies to the row its own author sent,
so in a group author A's early delete still never touches author B's row —
the §4.90 store scoping rides under the flush.

**The flush (the apply face).** The message-landing path drains the table
the moment the original row inserts, before the original's upsert event is
minted but with the drained events riding after it — the desktop sees
land-then-converge in order on one event stream. The application is the
live control arms' store path verbatim: a delete rides the author-scoped
`mark_remote_deleted`, an edit rides `apply_inbound_edit` /
`apply_outgoing_edit_by_signal` with the same reactions/edits attach and
the same event shapes, so a converged row is byte-identical whether its
control envelope arrived early or late. The flush matches fresh entries
only (TTL-expired ones leave silently — the original they waited for never
arrived, and applying a 6-minute-old edit is exactly the stale-write the
TTL exists to prevent); entries keyed to other rows stay buffered. The
buffer rides the §4.90 age gate (an aged delete is refused before it can
buffer), and the flush needs no age re-check of its own: entries live at
most 5 minutes, far inside the 24-hour window. Both faces stay bounded:
capacity + oldest-eviction + lazy TTL sweeps at every touch, so an original
that never arrives costs one self-expiring slot.

No wire change: the method/event/error enums are untouched, no new
capability, and `tests/schema_consistency.rs` stays green unchanged.

Tests: `early_remote_delete_buffers_and_applies_when_the_original_lands`
(the buffer's empty answer, the land-then-converge event order, the
converged row's status, the replay's silence); 
`early_edit_buffers_and_applies_when_the_original_lands` (the body rides
the entry, the flush rewrites the row in place — same id, new body,
editedAt set, two upserts on one stream);
`early_group_delete_applies_only_to_its_own_authors_row` (author B's row
landing first at the shared timestamp is never the target, author A's own
row converges); `early_control_buffer_stays_bounded_and_expires` (the
capacity eviction at 513, the backdated TTL sweep at the next touch).
Gates: 648 → 652 (lib 489 → 493), all green.

### 4.92 Startup sweep: stale pending sends land failed (contract revision 1.111, 2026-10-11)

A 'pending' outgoing row is a send whose completion never landed: the row
inserts before the engine call and `complete_outgoing_send` rewrites it
afterwards, so a process death in between leaves the row pending forever —
and nothing in the store can ever complete it again, because the only
writers of that transition live inside the process that just died. The
desktop's view of that row is a spinner that outlived its process; the
retry affordance (`messages.retryText`, the guarded failed→pending rearm,
§4.35) does not apply to pending, so the row is stuck in the one state with
no user escape hatch.

**The sweep.** `Store::open` runs one bounded statement after the schema
migrations, before any runtime, host lane, or CLI command sees the store:
`UPDATE messages SET status='failed' WHERE status='pending'`. The honesty
argument is ownership, not time: one connector instance owns one store (the
data-dir kernel lock at startup enforces that, ADR 0002), so no completion
for an orphaned pending row can ever arrive after the open — the state is
known-dead, not probably-dead. The failure state is the truthful answer AND
the useful one: the desktop renders the failed send with its retry action,
and the retry re-arm is the same guarded transition any other failure
rides. Rows in any other status are untouched: 'unknown' is a distinct
contract state (§4.x receipt vocabulary) with its own presentation, and the
delivered/read ladder is monotone above the sweep's reach.

**Idempotence is the semantics.** A reopen of an already-swept store finds
nothing pending and sweeps nothing; a re-armed row whose retry dispatch
itself died (another crash before completion) sweeps back to failed on the
next open — a pending row observed at open is by definition orphaned, so
the sweep cannot lie twice. The cost is one point UPDATE per open on the
status column (the receipt-ladder index covers it); a store with nothing
pending pays a single no-op scan.

No wire change: the method/event/error enums are untouched, no new
capability, and `tests/schema_consistency.rs` stays green unchanged.

Tests: `startup_sweep_migrates_stale_pending_sends_to_failed` (the reopen
sweeps exactly the stale pending row, the sent/failed rows untouched, the
swept row rearms through the guarded retry face, the re-armed row sweeps
back on the next orphaned open). Gates: 652 → 653 (lib 493 → 494), all
green.

### 4.93 The ending never regresses: lastMessageAt takes the max (contract revision 1.112, 2026-10-11)

Two store paths still wrote the conversation/account endings
(`last_message_at`) unconditionally: the live message insert and the send
completion. Both can carry a timestamp OLDER than what the summaries
already publish — Signal timestamps are per-sender clocks, so the live path
meets delayed deliveries (a peer back from offline lands yesterday's
message after today's), and the completion path re-stamps the row with the
engine-assigned upstream timestamp, which can sit below both newer rows and
the pre-send local clock value (skew, or a slow completion of an old
send). The regression was visible twice over: the conversation list re-sorts
by `last_message_at DESC`, so the dragged-back conversation jumped down and
then jumped up again when the next row landed, and both summaries showed an
ending older than messages the desktop already rendered.

**The guard.** Both writers take the max, in place: the ending columns move
only when the arriving value is newer (`CASE WHEN last_message_at IS NULL
OR last_message_at < ? THEN ? ELSE last_message_at END`), the exact
discipline the history-import backfill already ran (its "backfill never
regresses a summary" comment was the spec — this batch extends it to the
live paths it was protecting against). On the insert path the preview moves
together with the timestamp (the ending's two faces point at one row), and
the unread counters stay deliberately unconditional — an old message still
deserves its badge, and the 1.29 same-transaction bump discipline is
untouched. On the completion path the preview is deliberately untouched
(that path never wrote it — the preview already points at this row from the
pending insert, or at a newer row the guard now leaves alone). The account
ending rides the same CASE shape; the backfill/expire recomputes
(`MAX(last_message_at)` over the conversation rows) already agree with it.

No wire change: the method/event/error enums are untouched, no new
capability, and `tests/schema_consistency.rs` stays green unchanged.

Tests: `message_insert_and_send_completion_never_regress_the_ending` (the
delayed older row keeps the ending and still bumps the badge; the slow
completion at its upstream timestamp — the pre-guard regression — stays;
the stale-local-clock pending insert stays; the in-ladder completion still
advances). Gates: 653 → 654 (lib 494 → 495), all green.

### 4.94 The accountUnbound whitelist (contract revision 1.113, 2026-10-11)

The engine's unbound stop-ring (B4) pushes a single `accountUnbound`
notification (`{"method":"accountUnbound","params":{"account":"<number>"}}`)
when the server's 401 credential rejection halts the receive loop — official
linked-device semantics: the account waits for a re-link instead of an
infinite reconnect backoff. The notification shipped wire-additive with the
connector whitelist deliberately not consuming it, which left the account's
death reachable only through pull-shaped evidence: the host lane learned of
the dead link only when some later send failed with the authorization
signature, and until then the desktop kept rendering a live account whose
receive loop was parked for good.

**The arm.** `handle_upstream_line` now carries a dedicated dispatch arm for
the notification, replaying it onto the lane the unlink watcher already
consumes: a strictly-parsed non-empty `params.account` becomes
`EngineEvent::AccountUnauthorized` — the exact event
`account_unlink_watch_loop` handles by marking the account device-unlinked in
the shared store and surfacing `account.changed` on the host lane, so the
desktop sees the unlink with no new host surface and no supervisor change.
A malformed payload — missing `params`, missing, non-string or empty
`account` — carries no attributable identity and degrades to the
`unknownNotification` protocol warning instead of inventing an account; the
untrusted-launcher discipline (never log or fabricate identities) holds.
Accounts that share a proxy group stay untouched: the notification names one
account, and only that account's row moves.

No wire change on the connector's own API surface: the notification is
engine→connector only, the host-facing method/event/error enums are
untouched, and `tests/schema_consistency.rs` stays green unchanged.

Tests: `account_unbound_notification_marks_the_account` (exactly one
attributed event, no warning) and
`malformed_account_unbound_notifications_stay_unattributed` (missing
`params`, missing, non-string and empty `account` each yield only the
warning). Gates: 654 → 656 (lib 495 → 497), all green.

### 4.95 SEND_IDENTITY_UNVERIFIED: the identity gate gets a code (contract revision 1.114, 2026-10-11)

The engine's send path carries an identity-trust gate (P9): when the
recipient's identity key changed without confirmation, the send is refused —
the same cause official desktop refuses a send for — and the refusal folds
into the plain internal_error JSON-RPC face (`-32603`) whose `message` embeds
the fixed anchor `(UntrustedIdentityException)` (the exact wire literal
`Failed to send message: Identity unverified.
(UntrustedIdentityException)`). Before this revision the connector's
send-chain anchor classifier (§4.80) knew three anchors — rate limit, proof
challenge, server unavailability — so a safety-number refusal answered the
generic `UPSTREAM_ERROR` face: indistinguishable from noise, retryable=true,
and the desktop had no way to tell the user what actually unblocks the send
(confirm the safety number).

**The fourth anchor.** `upstream_error_send_class` gains the
`(UntrustedIdentityException)` anchor and a content-free
`EngineError::IdentityUnverified` variant — the same discipline as the three
transport siblings: fixed engine literal matched exactly, a message is only
ever classified never surfaced, no event (a per-recipient trust refusal never
touches account state), and the anchor classifier stays a standalone pure
function sharing no match set with the authorization matcher. The service
mapping answers `SEND_IDENTITY_UNVERIFIED` with retryable=false — confirming
the safety number is the only unblock, so re-driving an unchanged send is
refused again (the flag is display-side only; a mutating send is never
auto-retried). The schema error-code enum gains the code; the
`ENGINE_STRUCTURED_ERROR_CODES` allowlist is deliberately untouched — this
code travels the anchor-classification face, not the structured-code face,
so the two cannot collide (an embedded anchor in a longer message vs. an
exact allowlist literal).

Tests: `send_chain_classifier_maps_the_identity_anchor` (the exact engine
literal and the bare anchor classify; identity wording without the
parenthesized anchor and an anchor in the `code` slot do not), and the
§4.80 mapping table row (`SEND_IDENTITY_UNVERIFIED`, retryable=false, no
data object); `tests/schema_consistency.rs` covers the enum-set equality
automatically. Gates: 656 → 657 (lib 497 → 498), all green.

### 4.96 groups.update / groups.quit ride the engine write face (contract revision 1.115, 2026-10-12)

The engine batch landed the full `updateGroup` write family (expiration +
title + avatar tri-state + addMembers/removeMembers + the invite-link
access word + inviteLinkPassword) and the quit write under its own method
name `leaveGroup` (the engine face never carried a `quitGroup` literal).
Until this revision the connector answered both host faces with the honest
CAPABILITY_UNAVAILABLE debt refusal after local validation; the change-set
ladder validated shapes no engine would ever consume.

**The fold.** `prepare_groups_update` keeps the entire local ladder — the
at-least-one-change rule, the empty-member-array refusal, the byte bounds,
the avatar data-URI gate, the kind='group' resolution — and then folds the
validated change set onto the engine payload: groupName→`title`,
addMembers/removeMembers verbatim, the avatar tri-state verbatim, and
linkState mapped onto the engine's `addFromInviteLinkAccess` vocabulary
(enabled-without-approval→ANY, enabled-with-approval→ADMINISTRATOR,
disabled→UNSATISFIABLE — the official buildAccessControlAddFromInviteLink
enum). A change set naming any key outside that subset (groupDescription,
promoteAdmins/demoteAdmins, banMembers/unbanMembers, resetLink, the
permission trio, memberLabel/memberLabelEmoji) answers
CAPABILITY_UNAVAILABLE (retryable=false) naming the offending keys — the
honest whole-call refusal, never a partial application: the engine commits
the set as one change broadcast and the official client applies it
atomically, so folding a subset would silently shrink what the caller
asked for. `prepare_groups_quit` produces the `leaveGroup {account,
groupId}` payload (groupId = the row's peer_key, the terminate discipline).

**The error faces.** The engine's deny answers (-32603: unknown
account/group, non-member, a last-admin quit's user error) are
deterministic: both call sites now map `EngineError::Upstream` to
UPSTREAM_ERROR with retryable=false — a bare replay fails the same way
(the generic `into_api` default retryable=true was the pre-wiring
inheritance, wrong for a face whose every upstream error is a refusal).
The server-rejection family gets its own allowlist code:
`GROUP_UPDATE_FAILED` rides the `ENGINE_STRUCTURED_ERROR_CODES` allowlist
and maps with the engine's `data.status?` verbatim (the
CALL_CREDENTIALS_FAILED channel; `with_data` derives retryable=false —
the data carries no retryable key and the contract fixes it). A 409
revision race unblocks only after the caller re-reads the group state
(groups.get / conversations.list) and re-composes the change — never by
blind replay of the same set. A failed avatar upload after a landed group
change keeps the 1.89 partial-success face (GROUP_AVATAR_UPLOAD_FAILED,
data {retryable, textSaved}); the storage family keeps its STORAGE_*
mapping. Zero local writes on every path: the group row converges through
the receive projection (the §4.44 discipline).

Tests: `groups_update_quit_round_trip_and_guards` rewritten — the wired
avatar tri-state and the title/member/link change set assert the exact
dispatch payloads ({account, groupId, avatar}, the folded six-key set with
addFromInviteLinkAccess=ANY), the mixed set still refuses whole (naming
groupDescription, zero dispatch delta), the sentinel rows answer
GROUP_UPDATE_FAILED with data.status 403/409 verbatim, and the quit rides
leaveGroup ({account, groupId}) with the last-admin deny answering
UPSTREAM_ERROR retryable=false; the fixture's GROUP_UPDATE_FAILED
sentinels now answer on every updateGroup route and its quit arm became
`leaveGroup` ({status: "left"}). Lib tests join the rewrite:
`groups_update_and_quit_fold_onto_the_engine_face` (the folded payload, the
named-key refusal, validation outranking the gate, the quit payload),
`groups_update_avatar_tri_state_shapes_and_payload` (the tri-state rides
the payload verbatim), `groups_quit_preparation_resolves_the_ladder_and_
builds_the_payload`, and `group_update_failed_maps_with_the_status_pass_
through` (403/409/empty-data rides verbatim, retryable=false, the
data-less face content-free). Gates: 657 → 658 (lib 498 → 499), all green.

### 4.97 The conversation-state trio rides the storage face (contract revision 1.116, 2026-10-12)

The engine's P15b batch landed `getConversationState` /
`setConversationState` — the official Storage Service record's three
per-conversation bits (mutedUntilTimestamp / archived / markedUnread, one
record per Contact/GV1/GV2 conversation) as a read and a partial write.
Until this revision the connector surfaced none of the trio: the desktop
had no honest channel to mute, archive, or mark-unread through the linked
account, and the state lived only in the engine's cloud records.

**The faces.** `conversations.getState` (read lane, ReadOnly call) and
`conversations.setState` (send lane, Mutating) take the §4.47 blocked
face's addressing trio verbatim — accountId, conversationId, the
three-value kind enum through `validate_pinned_kind` — resolve the local
row through the shared ladder, and forward the resolved peer reference
plus kind. The empty-peer §4.42 self row fails closed locally (the
§4.56 guard). The set face carries the engine's partial-write grammar:
only the flags the caller sent ride the payload (absent = no write
intent for that bit; a flag value carries no tri-state of its own — the
host `null` folds to absent at the params deserializer), an intent with
no flags answers INVALID_REQUEST before any upstream call, and the
answer is the post-write three-bit projection read back (the engine's
official upload-success semantics). Both faces persist nothing locally —
the cloud record plus the desktop's own cache is the whole story; the
connector holds no state copy and mints no conversation.changed of its
own. The projection parser takes the engine's three-key contract
strictly: all three keys always ride (0/false = unset bit — a record the
cloud has not materialized reads as the all-default projection, the
honest idempotent read), and a missing or wrong-shaped key is a protocol
defect, not a default.

**The error faces.** No new codes. The engine's canonicalization
refusals arrive structured: a reference outside the storage taxonomy (a
contact the engine cannot resolve to a service id; a group id that is
not 32-byte master-key material — the fixture group's 12-byte id
included, a deliberate honesty mirror) answers CONVERSATION_NOT_RESOLVED
retryable=false; the outage family answers STORAGE_UNAVAILABLE /
STORAGE_WRITE_FAILED / STORAGE_READ_FAILED verbatim (retryable); the
bare -32603 parameter face (an account the engine no longer holds, a
malformed parameter the typed ladder cannot produce) folds into
UPSTREAM_ERROR retryable=false — the 1.115 deterministic-refusal
discipline, a replay fails the same way. The
mutating face rides the reaction-class discipline: an indeterminate
outcome answers SEND_OUTCOME_UNKNOWN and is never retried — the storage
write may already have landed. Advertised through the new
`conversation-state` capability tag (one capability for the read and
write halves, the 1.53 appendix discipline).

Tests: `conversation_state_round_trip_and_guards` — the handshake
advertises the two methods plus the tag, the all-default read, the
one-flag and two-flag writes composing over untouched bits (answers and
read-backs), the byte-exact dispatch pair pinning the
only-present-flags contract on the fixture log, the local refusal ladder
(empty intent, wrong flag shapes, kind enum on both faces, unknown
conversation/account), the CONVERSATION_NOT_RESOLVED shape pair (12-byte
group id; phone number addressed as group), and the STORAGE_UNAVAILABLE
sentinel pair with retryable=true. Lib tests join:
`prepare_conversation_state_faces_ladder_and_payload` (the addressing
trio, the only-present-flags payloads, the empty-intent refusal, the
wire-order ladder) and `conversation_state_projection_parses_strictly`
(the strict three-key parse, missing and wrong-shaped keys are protocol
defects). The fixture gains the in-memory conversation-state store with
the engine's canonicalization faces and the
`[fixture-state-unavailable]` outage sentinel. Gates: 654 → 657 (lib
499 → 501), all green — the total recalibrates at this revision (§1):
the per-batch deltas were honest, the running total was not.

### 4.98 The group invite-link family rides the engine link faces (contract revision 1.117, 2026-10-12)

The engine's P13 batch landed the full group invite-link family —
`generateInviteLinkPassword`, `buildInviteLink`, `parseInviteLink`,
`previewGroupViaLink`, `joinGroupViaLink`, `cancelJoinRequest` — the
official GroupInviteLink proto round trip (contentsV1{groupMasterKey,
inviteLinkPassword} → standard base64 → web-safe → the official
`https://signal.group/#<code>` URL, web-safe alphabet throughout the
code). Until this revision the desktop had no honest channel to mint,
share, decode, preview, join through, or withdraw a join request on a
group link: the material lived only engine-side.

**The faces.** Six methods behind the new `group-invite-links`
capability tag. `groups.generateInviteLinkPassword` takes the
empty-params shape and answers `{password}` — a fresh 16 CSPRNG bytes as
standard base64 (24 chars), two calls never coincide;
`groups.parseInviteLink` takes only the link and answers
`{groupId, password}` — a pure decode across the three official URL
forms and the bare invite code. Both are account-less read faces riding
the default group's engine (the §4.32 getStickerPackManifest
anonymous-face discipline). `groups.buildInviteLink` resolves the group
row through the shared ladder before any engine call (a direct row
answers INVALID_REQUEST, an unknown id CONVERSATION_NOT_FOUND) and
takes the optional password: with one the build is pure construction
(the official explicit-password arm — fake and real same code path);
without one the engine reads the server group state's link password and
a group that has not enabled its link answers GROUP_INVITE_LINK_UNAVAILABLE
— the honest not-set state, no data. The password (standard base64 of
1–64 bytes, the official password is 16) and the link are single-pass
material forwarded verbatim: the connector never re-derives, re-encodes,
shape-gates, logs, or persists either — the engine's decode gate
refusing other shapes is the deterministic UPSTREAM_ERROR face
(retryable=false, the 1.115 discipline). `groups.previewViaLink`
(accountId + link) answers the official pre-join projection
{groupId, memberCount, revision, addFromInviteLink, approvalRequired,
pendingAdminApproval, name?, description?} — the connector persists
nothing, the preview is the answer. `groups.joinViaLink` (accountId +
link) answers `{status: "joined" | "requested"}` per the group's
approval gate, and `groups.cancelJoinRequest` answers
`{status: "cancelled"}`; both are send-lane Mutating calls whose
dispatch records ride record-before-parse — a refused join still leaves
its dispatch record, the honest log of what the engine was asked.

**The error faces.** Two new structured-allowlist codes, both
retryable=false: GROUP_INVITE_LINK_UNAVAILABLE (the build's not-set /
forbidden face, `data.status?` verbatim when the engine attaches it,
data-less otherwise) and GROUP_INVITE_LINK_PREVIEW_FAILED (the server
refused the pre-join read, `data.status?` verbatim). The join/cancel
server-rejection channel rides the existing GROUP_UPDATE_FAILED code
(the join is a group-state write; `data.status` verbatim). The outage
family answers STORAGE_UNAVAILABLE verbatim, retryable=true, on all
four account-scoped faces. Malformed links — every parse-family
tolerance failure — fold into UPSTREAM_ERROR retryable=false on the
parse, preview, join, and cancel faces alike. The mutating pair keeps
the reaction-class discipline: an indeterminate outcome answers
SEND_OUTCOME_UNKNOWN and is never retried — the join request may
already sit on the group's admin queue.

Tests: `groups_invite_link_round_trip_and_guards` — the handshake
advertises the six methods plus the tag, the password face's uniqueness
pair, the build → parse round trip across the three official URL forms
and the bare code, the full preview projection, the joined and
approval-gated requested verbs plus the cancel, the five-write dispatch
log pinned before the failure faces (record-before-parse), the
structured refusal ladder (preview 404, join 403, cancel 409, build
403, build data-less unset), the outage sentinel pair with
retryable=true, and the malformed-link pair folding into UPSTREAM_ERROR
retryable=false on both the parse and the join face. Lib tests join:
`prepare_group_invite_link_faces_ladders_and_payloads`,
`group_invite_link_projections_parse_strictly`, and
`group_invite_link_codes_map_with_the_status_pass_through`. The fixture
gains a module-level proto codec (varint fields, web-safe base64) with
the build/parse engines and the three 16-byte password sentinels
(forbidden / preview-failed / approval-requested). Gates: 657 → 661
(lib 501 → 504), all green.

### 4.99 The delete-for-me upstream leg rides the engine sync face (contract revision 1.118, 2026-10-12)

The engine's P15c 项3 batch landed `deleteForMeConversation` — the
official deleteConversation sync (SendMessage.preload.ts
getDeleteForMeSyncMessage) a desktop fires when it clears a conversation,
so the account's other linked devices converge their copies too. The
connector's §4.83 batch already carries the inbound half of that family
(the `syncMessage.deleteForMe` receive projection, the store mirror, the
`message.deleteForMe` forward); the outbound half had no face at all —
the desktop's own clear-decision could never reach the phone. One method
closes the family: `messages.deleteForMe` behind the new `delete-for-me`
capability tag.

**The two arms.** The addressing trio is the §4.97 shape
(accountId/conversationId/kind) resolved through the shared ladder — the
conversation row's peer reference is exactly the identity the engine's
toConversationIdentifier shape carries (a service-id string or an e164
for kind=contact, the 32-byte master key for kind=group), and the §4.56
empty-peer guard keeps the self row out. `localOnly` absent/false is the
full-clear arm: `mostRecentMessages` carries 1..=5 {author, timestamp}
entries (the official getMostRecentAddressableMessages().slice(0, 5)
mirror — a larger list answers INVALID_REQUEST before any upstream call;
a destructive intent is never silently truncated) with the optional
`mostRecentNonExpiringMessages` companion ≤5 (the official
areAnyDisappearing companion). `localOnly: true` is the local-only arm
and must carry no entries at all (the official local-only branch has no
message position — the other devices only clear their local rows).
kind=legacyGroup refuses deterministically (the official
getConversationIdentifier throw for non-GV2 groups). The entries forward
verbatim — the phone-side row lookup keys on the exact
(author, sentTimestamp) strings, so the connector never re-derives an
author, an entry shape the typed ladder cannot produce is refused
locally (zero timestamps, empty or oversized authors), and nothing is
logged or persisted beyond the engine's own dispatch record.

**The class.** Receipt-class, the §4.89 syncRead/syncViewed discipline
verbatim: send lane, account-scoped mutating, and any engine call
failure — including the engine's pre-flight -32603 refusal — degrades to
{status: "unknown"} with no error object and is never retried (the sync
message may already ride the self-send lane). Zero local writes: the
§4.83 receive mirror stays the inbound channel, the connector's rows
keep their mirror state, and the desktop's own convergence rides the
same `message.deleteForMe` forward it already consumes — this face is
the outbound leg only. No new error codes (INVALID_REQUEST before any
upstream call; the degradation otherwise), no store shape moves.

Tests: `messages_delete_for_me_rides_the_engine_sync_face` — the
handshake advertises the method plus the tag, the full-clear arm's
verbatim dispatch (account, resolved peer reference, both entry lists),
the local-only arm's bit-only dispatch, both arms pinned in wire order
against the fixture log, the six grammar refusals answering
INVALID_REQUEST with nothing upstream, and the crash sentinel
degrading to `unknown` with no error object. Lib tests join:
`prepare_messages_delete_for_me_ladders_the_official_grammar` — both
arms' exact upstream shapes, the group-kind master-key resolution, and
the grammar ladder (empty/oversized lists, entries or companion on the
local-only arm, zero timestamp, empty author, legacyGroup, unknown
conversation). The fixture gains the `deleteForMeConversation` stub with
the fail-closed param gate, the SEND_LOG dispatch record, and the 427
crash sentinel (disjoint from the 421/423/424/425/426 space). Gates:
661 → 663 (lib 504 → 505), all green.

### 4.100 The blocked read carries the block stamp: `blockedAtTimestampMs` (contract revision 1.119, 2026-10-12)

The engine's 窗D B7 batch closed its own B7 gap: the blocked read face
now projects `blockedAtTimestampMs` off the official storage-record bits
(ContactRecord.blockedAtTimestamp field 27 / GroupV2Record field 13,
official
`ts/services/storageRecordOps.preload.ts:1058-1075` consumption). The
connector's §4.45 `conversations.getBlocked` projection was keyed to the
old answer shape, so the stamp the engine now sends was falling on the
floor — the desktop could see that a conversation is blocked but never
since when. The projection picks the key up per entry, additive only.

**The shape.** `BlockedConversation` gains the optional
`blockedAtTimestampMs` (u64, present only when the record holds a
positive stamp — the engine sends the key only then; 0 is the official
"unknown" posture and stays absent, the getCallLinks
deletedAtTimestampMs family precedent, `Ms` suffix included). A GV1
record never carries the bit (the StorageService proto has no such
field for GV1 — the engine's recorded boundary), so the legacyGroup
entry keeps the bare shape. The parse is shape-tolerant per key
(absent/non-shape → absent — the read-face per-key degradation; an
answer from a pre-B7 engine reads byte-identically), the cap and the
drop rules are untouched, and the write leg (conversations.setBlocked)
needs nothing: its write-after echo rides the same projection through
the same parse.

**The wire.** Record-face additive only: one optional key on the
blocked entry, old hosts ignore it as an unknown JSON member. No new
method, error code, or capability tag; no store move (the read persists
nothing, as before). The schema's getBlocked description paragraph
restates the projection (and retires the "deliberately no
blockedAtTimestamp in v1" sentence that 1.119 makes false).

Tests: the lib test `blocked_conversations_result_caps_and_projects`
gains the projection ladder (a positive stamp rides verbatim, 0 and a
non-shape value stay absent, the key-less pre-B7 shape reads the same,
the GV1 entry stays bare); the blocked-read integration assertions gain
the pass-through pair (the fixture's contact entry carries the stamp,
the legacyGroup entry never does). The fixture's BLOCKED_STATE contact
entry gains the stamp. Gates: 663 held (lib 505 held — the projection
ladder and the pass-through pair extend two existing tests in place,
the baseline grows in assertions, not in test count), all green.

### 4.101 The account-settings write face gains the linkPreviews bit (contract revision 1.120, 2026-10-12)

The engine's 窗D B6 批1 (48bc273) closed the engine half of the
link-preview account setting: `setAccountConfiguration` accepts the
optional `linkPreviews` boolean (the official AccountRecord field 11)
and `getConfiguration` projects it back with the four existing flags
(the five-boolean read face). The connector's §4.45 write face was
keyed to the old four-key shape, so the desktop could not express the
bit at all — a settings pane carrying the link-preview toggle would
silently lose it at the connector boundary.

**The shape.** `AccountsUpdateConfigurationParams` gains the optional
`link_previews` (`linkPreviews` on the wire). It rides the existing
ladder unchanged: the at-least-one rule now counts five keys (a lone
`linkPreviews` write is legal), the key forwards upstream only when
provided (`upstream["linkPreviews"]`, the partial-write semantics — the
engine writes exactly the provided fields), and the non-boolean shape
fails params deserialization with INVALID_REQUEST before anything runs.
No local row: the setting lives in the engine-side account record, the
same no-local-persistence discipline as the other four keys.

**The read mirror already spoke the name.** The §4.87
`settings.configurationSync` event has carried the `linkPreviews` key
since contract 1.106 (the engine's syncMessage.configuration
projection), so the 1.120 work is the write half only — but the
integration now pins the linkage explicitly: the fixture delivers one
cross-device configuration sync beside the write assertions, and the
test asserts the event's `linkPreviews` key lands with the same name
the write face takes. One concept, one wire word, both directions.

**The wire.** Additive only: one optional boolean on an existing params
def (old hosts ignore it as an unknown member — `deny_unknown_fields`
is a connector-side gate, not a host-side one). No new method, error
code, or capability tag; the face stays behind `account-settings`. The
fake fixture type-gates the key mirror of the engine's own bool face
(non-boolean → -32603), defense in depth behind the connector's serde
gate.

Tests: the lib prepare ladder gains the lone-linkPreviews arm (a legal
at-least-one satisfier forwarding only its key) and the absent-key arm;
the integration gains the forwarded pair (the mixed write carries
`linkPreviews: true` beside readReceipts, the lone write forwards only
`linkPreviews: false`), the non-boolean refusal, and the read-mirror
linkage block. Gates: 663 held (lib 505 held — assertions extend the
two existing tests in place), all green.

### 4.102 The media-ingest gate flips to pass-through (contract revision 1.121, 2026-10-12)

The registry's `effective_media_ingest` — since the kt-engine era a
forced downgrade that kept `--ignore-attachments` on the argv, skipped
the media governor, and answered every media method
`CAPABILITY_UNAVAILABLE` however the launcher decided — now passes the
launcher's `--media-ingest` decision through verbatim. This is a
main-path policy flip, not a new execution surface: every media face
(chunked streaming triple, `history.attachmentDownload`,
`attachments.download`), the media governor, and the 1.98
advertisement gate are exactly as they were; what changes is who owns
the decision. The driving scenario is cross-device sync: the account's
own linked devices push media-bearing state, the engine projects the
receive locator face (the 1.61 closure — `locator` keys ride the
attachment pointer projection), and an armed runtime ingests media
honestly instead of silently degrading to metadata-only.

**取代理由（the decision chain）.** The downgrade was recorded 2026-10-03
when the kt-engine had no attachment ingest at all: arming ingest would
have advertised a face that failed for every call, and the dev desktop
arms ingest unconditionally, so the downgrade kept it booting (the two
superseded red lines — the §5 launch bullet and the §6.7
media-limitation paragraph — are struck through in place above with
pointers here). Three facts retire the premise: (1) the engine ships
the receive locator projection, so rows land with downloadable
material; (2) the bounded-download chain passed its acceptance gates
(1.17 chunked channel, 1.57 history chain, 1.61 live chain, the
quota+TTL governor bounding retention); (3) the 1.98 handshake gate
already ties the `attachment-download` advertisement to this same
effective decision, so a pass-through cannot advertise a face the
runtime cannot serve — the honesty the downgrade provided is now
provided by the advertisement gate itself.

**Invariants held across the flip.** Locator-less legacy rows stay
permanently non-downloadable: the locator ladder answers
`INVALID_REQUEST` before any network traffic (never a guessed
download), the honest copy — an old row is indistinguishable from a
pointer-less one and both refuse identically. Pointer material never
logs: the download chain's diagnostics stay content-free (the §4.38
discipline, unchanged). Queues stay bounded: the media governor's
quota+TTL retention arms exactly when ingest arms (the §10.2-era
bounds); the dev desktop's unconditional arming now gets the real
bounded machine instead of the silent no-op.

**The wire.** No new method, error code, or capability; no store move.
The unarmed shape (the packaged default) is byte-identical to before —
same argv, same CAPABILITY_UNAVAILABLE answers, same tag-less
handshake. The armed shape changes argv (the flag drops) and
advertisement (the tag rides), which is the flip's entire point.

Tests: the registry unit flips (`media_ingest_passes_through`: true
passes through, false stays off); the armed-advertisement handshake
test and the unarmed harness assertion already pin both advertisement
sides and stay green unchanged. Stale downgrade wording swept from the
launcher docs, the config field, the capability tables, and both §5/§6.7
red lines (struck through, pointered here).

真机回归（local real-process regression, this commit）: release build
served as the regression binary; `serve --media-ingest` against the
fake engine answered `handshake.capabilities` containing
`attachment-download` (armed) while the same binary without the flag
omitted it (unarmed), and `messages.attachments.open` answered
`CAPABILITY_UNAVAILABLE` only on the unarmed side — the flip's two
observable faces end to end. Gated behind the full local gate suite
(fmt + clippy -D warnings + cargo test + release build), all green.

### 4.103 The device plane reaches the host: `accounts.getDevices` / `accounts.unlinkDevice` (contract revision 1.122, 2026-10-12)

The engine's 窗D B13 batch shipped the device-plane faces — the GET
v1/devices projection with the engine-side device-name decryption
(the official `encryptDeviceName` pair, decrypted with the account's own
ACI identity key) and the server-side unlink (DELETE v1/devices/{id},
the official unlink endpoint) — and this batch wires both into the
host contract as `accounts.getDevices` and `accounts.unlinkDevice`,
advertised through the new `device-management` capability tag (one
capability for the read and write halves, the 1.53 appendix
discipline). Until now the desktop could register device capabilities
(§4.81) but could neither list nor unlink the account's devices; the
unlink decision previously had only the §4.17 chain (the account's own
device being unlinked remotely, an engine-event worker concern), with
no outbound leg for the user's own choice.

**The read face.** `accounts.getDevices {accountId}` answers
`{devices: [{deviceId, name?, registrationId?, lastSeenMs?,
createdAtCiphertext?}]}` in server order, including the account's own
primary device. The engine's bare `account` identity never crosses the
connector; the decrypted device name is the honest optional — an
encrypted name that is absent or undecryptable answers no `name` key
(the official decrypt-failure domain, where the desktop warns and moves
on). The projection is bounded: at most 32 entries (`MAX_DEVICE_LIST_
ENTRIES`; the official per-account device ceiling is a small handful,
so the cap is a hostile/broken-engine guard, not a working-set size —
entries beyond it truncate, the §4.45 blocked-list cap discipline); an
entry whose `deviceId` is non-numeric drops (the tolerant read-face
rule); and a projected `deviceId` of `0` fails the whole face
INVALID_REQUEST fail-closed — 0 is the libsignal "no device" hole,
never a real server device, and a list claiming it must not cross the
connector as if it were one. Read lane, non-mutating, no local
persistence, no logging.

**The write face.** `accounts.unlinkDevice {accountId, deviceId}`
answers `{status: "unlinked"}` on the definite success. The server
owns the authorization (a primary unlinks any linked device, a linked
device unlinks only itself); the over-reach and primary-delete
refusals surface as DEVICES_FAILED with `error.data.status` carrying
the server HTTP status verbatim (the CALL_CREDENTIALS_FAILED honest
pass-through channel). `deviceId` 0 is refused INVALID_REQUEST
locally, before any upstream call — the engine would answer its plain
-32603 there, and the connector prefers the precise local answer over
folding it into the generic upstream family. The upstream DELETE is
mutating and its outcome can be lost in transport: an indeterminate
outcome answers SEND_OUTCOME_UNKNOWN and is never retried (the
AGENTS.md red line — an unlink whose delivery is unknown may already
have happened server-side, and a blind replay could unlink a device
the user did not choose). Send lane, account-scoped mutating (the
same-account mutex and the delete drain barrier serialize it); the
unlink converges to the account's other devices through the official
device-link sync — the connector adds no local unlink mirror.

**The wire (contract 1.122).** Additive-only: two methods
(`accounts.getDevices`, `accounts.unlinkDevice`) join the request
enum with their params shapes (`accountsGetDevicesParams`,
`accountsUnlinkDeviceParams` — `deviceId` integer 1..4294967295); two
error codes join the schema enum — DEVICES_FAILED (retryable=false,
`data.status?` verbatim) and DEVICES_UNAVAILABLE (retryable=true, the
network/transport family) — mirrored verbatim into the engine.rs
allowlist (`ENGINE_STRUCTURED_ERROR_CODES`, the two-sides-total test
keeps both faces honest); the `device-management` feature tag rides
the handshake; the error-`data` description gains the 1.122 member.
No event, no store move, no engine-method gap: the connector calls
the engine's `getDevices`/`unlinkDevice` exactly as the B13 batch
pinned them.

Tests: the service unit pins the projection (cap, drop, id-0 refusal,
protocol-defect answers) and both retryability faces plus the
`data.status` pass-through; the methods-table test pins the lane and
mutating classification; the integration test round-trips the read
(ghost entry drops, nameless primary and absent-optionals project,
resolved account rides upstream), the write (exact upstream frame,
DEVICES_FAILED data.status, the local id-0 ladder, unknown account),
and the red line (crash-in-flight answers SEND_OUTCOME_UNKNOWN across
the restart, exactly one crashing frame). Gated behind the full local
gate suite (fmt + clippy -D warnings + cargo test + release build),
all green.

### 4.104 The GV1→GV2 migration reaches the host: `groups.migrate` (contract revision 1.123, 2026-10-12)

The engine's B8 batch shipped `migrateGroup` — the official
`initiateMigrationToGroupV2` face: the engine derives the new GV2
master key from the legacy id (the HKDF derivation primitive is defined
over arbitrary bytes, so the engine pins no 16-byte id length) and
answers `{groupId, revision}`, with the already-migrated fallback probe
(the put lost its race to a concurrent migration; the official
`hasV1GroupBeenMigrated` semantic) presenting as the same honest
answer. This batch wires it into the host contract as `groups.migrate`,
advertised through the existing `group-management` tag widened in
place (the 1.82 precedent — the migration is the group life cycle's own
face). Until now the desktop could join, update, quit, and terminate
groups but could not migrate a legacy one; the GV1→GV2 decision had no
outbound leg.

**The shape.** `groups.migrate {accountId, groupId, title?,
expiration?, avatar?, members}` — the legacy group id is addressed
directly in standard base64 (no local conversation row resolves; the
desktop owns the GV1 roster it hands over, and the migrated group
converges through contacts.sync like every group row). The local shape
ladder refuses before any upstream call: a non-base64 or empty id
(INVALID_REQUEST — the engine's shape face made precise), a non-empty
title over the 256-byte `groupName` bound, an empty roster (the
migration must carry the V1 member list — the official migrate always
ships it), and the §4.44 roster bounds (1..1001 entries of 1..128
characters). The avatar tri-state (absent = keep, null = remove, a
data URI = replace) runs the §4.45 `validate_avatar_data_uri` gate
locally, so a malformed URI answers INVALID_REQUEST instead of folding
into the generic upstream family; the fixture-side dispatch log
digest-summarizes the avatar (the engine's "zero avatar bytes in the
dispatch log" discipline mirrored).

**The failure faces.** GROUP_MIGRATION_FAILED joins the structured
whitelist (engine.rs allowlist + schema enum mirrored verbatim):
retryable=false — the same group state refuses the same way, and a 409
race unblocks only after re-reading the group state — with the engine's
`data.status?` riding the honest HTTP pass-through channel (the
CALL_CREDENTIALS_FAILED / DEVICES_FAILED channel). The avatar
partial-success family rides the existing GROUP_AVATAR_UPLOAD_FAILED
and the transport family the existing STORAGE_UNAVAILABLE. A mutating
upstream write whose outcome can be lost in transport: an
indeterminate outcome answers SEND_OUTCOME_UNKNOWN and is never
retried (the AGENTS.md red line — a migration whose delivery is
unknown may already have created the GV2 group server-side).

Tests: the service unit pins the prepare ladder (base64 gate, title
bound, roster bounds, the optional-keys-absent-stay-absent frame, the
avatar tri-state) and the result parse plus the GROUP_MIGRATION_FAILED
faces; the methods-table test pins the lane and mutating
classification; the integration test round-trips the migration (the
change set rides upstream verbatim, the answer projects {groupId,
revision}), the local ladder (malformed id / empty roster / over-bounds
roster refuse with zero upstream frames, unknown account), the server
rejection (data.status 409 verbatim), and the red line
(crash-in-flight answers SEND_OUTCOME_UNKNOWN across the restart,
exactly one crashing frame). Gated behind the full local gate suite
(fmt + clippy -D warnings + cargo test + release build), all green.

### 4.105 The contact card reaches the host: `messages.sendContact` (contract revision 1.124, 2026-10-12)

The engine's window-D batch shipped `sendContact` — the official
DataMessage.contact single-entry face (the desktop's
`SendMessage.preload.ts` toProto contact section + the
`EmbeddedContact.std.ts` numberTo*Type vocabulary, same position). This
batch wires it into the host contract as `messages.sendContact`,
advertised through the new `send-contact` capability tag (contract
1.34 discipline — desktops gate the surface on the tag, so an old
connector answers a clean capability gap instead of
`METHOD_NOT_ALLOWED`). Until now the desktop could send text,
attachments, stickers, stories, reactions, and pins but could not share
a contact card; the address-book share had no outbound leg.

**The shape.** `messages.sendContact {accountId, conversationId,
contact}` — the conversation row's kind decides the leg (the
calls.sendSignal 1:82 addressing precedent: the connector already holds
the kind, so no new parameter). A direct row routes the engine's 1:1
`sendContact` (`{account, recipient: the row's peer reference,
contact}`); a group row routes the `groupId` group leg — one upstream
call fans the card to every group member through the sender-key
channel. `contact` is the official ContactAttachment shape verbatim:
`name` (the six official components, all optional), `number`/`email`
(the value entries), `address` (the official postal nine-field shape),
`organization` (an empty string is the official falsy-absent and drops
from the upstream frame), `avatar` (a data URI whose bytes ride the CDN
uplink as an attachment pointer — the official
EmbeddedContactWithUploadedAvatar upload face, the engine failing the
whole call when the upload chain fails; the bytes never touch a
connector store). The card gates
are fail-closed before any upstream call (the engine's parse face
mirrors them, but a local refusal never spends a fan-out frame): an
all-empty card answers INVALID_REQUEST (it carries nothing —
empty-string name components count as absent, the official falsy
mirror); entry arrays hold at most 64 entries; every string is bounded
at 128 characters with the phone/email values non-empty; the `type`
vocabularies are the official numberTo*Type words (number/email:
HOME | MOBILE | WORK | CUSTOM, address: HOME | WORK | CUSTOM — the
postal enum never had a MOBILE; the official client silently folds
out-of-vocabulary words to HOME and the engine refuses instead, the
textStyle precedent, §5 deviation recorded); the avatar data URI rides
the §4.45 `validate_avatar_data_uri` gate so a malformed URI answers
INVALID_REQUEST first. The fixture-side dispatch log digest-summarizes
the avatar (the engine's "zero avatar bytes in the dispatch log"
discipline mirrored); the rest of the card is the caller's own text
data and rides verbatim.

**The answer and the failure faces.** The response is `{sentAt}` — the
engine's send timestamp, the calls.sendSignal ack shape; the connector
keeps no send rows for the face (the conversation timeline converges
through the receive projection like every outbound send). No new
structured codes: the face rides the existing upstream send family, and
a mutating upstream write whose outcome can be lost in transport answers
SEND_OUTCOME_UNKNOWN (retryable=false) and is never retried (the
AGENTS.md red line — a card whose delivery is unknown may already sit
in the recipients' timelines).

Tests: the service unit pins the prepare ladder (the all-empty and
all-falsy refusals, the type vocabularies both legs, the entry and
scalar bounds, the avatar gate, the organization fold) and the leg
routing (the direct row's peer reference, the group row's groupId);
the methods-table test pins the lane and mutating classification and
the capability list; the integration test round-trips both legs (the
direct frame carries the recipient and the card verbatim with the
avatar digest-summarized, the group frame carries the groupId; the
{sentAt} ack echoes the engine timestamp), the local ladder (seven
refusals with a byte-frozen dispatch log), the unknown account, and the
red line (crash-in-flight answers SEND_OUTCOME_UNKNOWN across the
restart, exactly one crashing frame). Gated behind the full local gate
suite (fmt + clippy -D warnings + cargo test + release build), all
green.

### 4.106 The polls reach the host: `messages.sendPollCreate` / `sendPollVote` / `sendPollTerminate` (contract revision 1.125, 2026-10-12)

The engine's window-D A7 batch shipped the poll send trio — the
official DataMessage pollCreate/pollVote/pollTerminate faces (the
desktop's PollCreateSchema / PollVoteSchema content gate,
types/Polls.dom.ts, same position). This batch wires all three into the
host contract under one capability tag, `polls` (the 1.34 discipline —
desktops gate the surface on the tag so an old connector answers a
clean capability gap instead of `METHOD_NOT_ALLOWED`). Until now the
desktop could receive and render polls but could not create, vote in,
or terminate one; the poll life cycle had no outbound leg.

**The shape.** `messages.sendPollCreate {accountId, conversationId,
question, options, allowMultiple?}` / `messages.sendPollVote
{accountId, conversationId, targetAuthor, targetTimestamp,
optionIndexes, voteCount?}` / `messages.sendPollTerminate {accountId,
conversationId, targetTimestamp}` — all three are group-conversation
faces exclusively (the official desktop ships polls in groups; the 1:1
create form rides a feature flag that defaults off, vote/terminate have
no 1:1 form at all), so the connector resolves the conversation row and
refuses any non-group kind before any upstream call; the row's peer
reference becomes the upstream groupId. The content gates are the
official schema verbatim and run locally fail-closed (the engine's
poll_question_face / poll_options_face / poll_option_indexes_face pin
the same bounds, but a local refusal never spends a group fan-out): the
question 1..=200 characters within 2048 bytes (the official
MAX_MESSAGE_BODY_BYTE_LENGTH); the options 2..=10 entries of 1..=100
characters each within 2048 bytes; the selection optionIndexes 0..=9
with at most 10 entries, an empty array the legal clear-vote action;
the anchor targetTimestamp a positive integer (the official
`z.number().int().positive()`), targetAuthor the §4.44 member-id bound;
voteCount positive, absent = the engine's 1 default; allowMultiple
rides when present, absent = the engine's false default (the official
single-selection form).

**The answer and the failure faces.** The upstream result body is
empty — the poll's identity is the send timestamp the receive
projection converges — so the answer follows the remoteDelete status
discipline: a definite success answers the explicit
`{"status":"sent"}`, an indeterminate outcome the explicit
`{"status":"unknown"}`, a definite error propagates; nothing retries
(the AGENTS.md red line — a poll whose delivery is unknown may already
sit in the group timeline). No new structured codes: the faces ride the
existing upstream send family.

Tests: the service unit pins the prepare ladder (the content bounds,
the selection domain, the zero anchor, the direct-conversation
refusal, unknown account) and the frames (the resolved account + group
peer key, allowMultiple/voteCount absent-stay-absent); the
methods-table test pins the three lanes and the capability list; the
integration test round-trips all three faces upstream (create with and
without allowMultiple, vote with a selection and the empty clear-vote,
terminate; every frame carrying the resolved account and groupId), the
local ladder (five refusals with a byte-frozen dispatch log), the
unknown account, and the red line (crash-in-flight answers
{"status":"unknown"} across the restart, exactly one crashing frame).
Gated behind the full local gate suite (fmt + clippy -D warnings +
cargo test + release build), all green.

### 4.107 The group create reaches the host: `groups.create` (contract revision 1.126, 2026-10-12)

The engine shipped `createGroup` — the official createGroupV2 face: the
engine derives the new group master key from its CSPRNG and answers
`{groupId, revision}` (revision 0 — a fresh group's first state). This
batch wires it into the host contract as `groups.create`, advertised
through the existing `group-management` tag widened in place (the 1.82
precedent — the create is the group life cycle's own first face).
Until now the desktop could join, update, migrate, quit, and terminate
groups but could not create one; the new-group decision had no outbound
leg.

**The shape.** `groups.create {accountId, title, expiration?, avatar?,
members?}` — `title` is required non-empty within the 256-byte
`groupName` bound (the official createGroupV2 `name: string` is
mandatory the same way); `expiration` the optional timer seconds; the
avatar tri-state (absent = keep, null = remove, a data URI = replace)
runs the §4.45 `validate_avatar_data_uri` gate locally; `members` the
optional initial roster — absent creates the empty group (the engine's
own default), present it carries the §4.44 bounds (1..1001 entries of
1..128 characters), and an explicit empty array is the engine's own
refusal mirrored locally so the shape faces stay total. A malformed
avatar URI answers INVALID_REQUEST before any upstream call.

**The failure faces.** GROUP_CREATE_FAILED joins the structured
whitelist (engine.rs allowlist + schema enum mirrored verbatim):
retryable=false — the same group state refuses the same way — with the
engine's `data.status?` riding the honest HTTP pass-through channel
(GROUP_MIGRATION_FAILED 1.123 channel). The avatar partial-success
family rides the existing GROUP_AVATAR_UPLOAD_FAILED and the transport
family the existing STORAGE_UNAVAILABLE. A mutating upstream write
whose outcome can be lost in transport: an indeterminate outcome
answers SEND_OUTCOME_UNKNOWN and is never retried (the AGENTS.md red
line — a create whose delivery is unknown may already have created the
group server-side; a blind replay would create a second one).

Tests: the service unit pins the prepare ladder (the title bound, the
roster bounds both arms, the optional-keys-absent-stay-absent frame,
the bare frame) and the result parse plus the GROUP_CREATE_FAILED
faces; the methods-table test pins the lane and mutating
classification; the integration test round-trips the create (the bare
and rostered frames, the answer projecting {groupId, revision}), the
local ladder (four refusals with a byte-frozen dispatch log), the
server rejection (data.status 400 verbatim), the unknown account, and
the red line (crash-in-flight answers SEND_OUTCOME_UNKNOWN across the
restart, exactly one crashing frame). Gated behind the full local gate
suite (fmt + clippy -D warnings + cargo test + release build), all
green.

### 4.108 The group-call era update reaches the host: `calls.sendGroupCallUpdate` (contract revision 1.127, 2026-10-12)

The engine's window-A batch shipped `sendGroupCallUpdate` — the official
DataMessage.groupCallUpdate face (the group-call start/end signaling the
desktop's `CallingFrame` era model rides, same position). This batch
wires it into the host contract as `calls.sendGroupCallUpdate`,
advertised through the existing `call-signaling` tag widened in place
(the 1.82/1.88 precedent — the era update is the same signaling pipe's
group-era face, no new capability bit). Until now the desktop could
forward 1:1 and group call signaling envelopes but could not announce a
group-call era change; the group call life cycle had no update leg.

**The shape.** `calls.sendGroupCallUpdate {accountId, conversationId,
eraId}` — the conversation row must resolve to a group kind before
anything travels (group call signaling has no direct form — 1:1 call
signaling rides `calls.sendSignal`, so a direct row answers
INVALID_REQUEST locally); the row's peer reference is the upstream
groupId, gated locally as standard base64 decoding to exactly 32 bytes
(the engine's `decode_group_id` face made precise — a short-key row
never spends a fan-out frame); `eraId` is the mandatory non-empty RingRTC
era identity string (≤256 characters, the engine's shape gate mirrored).

**The answer and the failure faces.** The engine's success body is
empty — the update's identity is the era string itself — so the definite
success answers the explicit `{"status":"sent"}` (the remoteDelete
discipline), and an indeterminate mutating outcome answers
SEND_OUTCOME_UNKNOWN (retryable=false) and is never retried (the
AGENTS.md red line — an era update whose delivery is unknown may already
sit in the group's call state). No new structured codes: the faces ride
the existing upstream send family.

Tests: the service unit pins the prepare ladder (the group-kind gate,
the 32-byte master-key gate, the eraId bound, the frame verbatim) and
the refusals; the methods-table test pins the lane and mutating
classification; the integration test round-trips the update (the frame
carrying the resolved account, the master key, and the era string; the
{"status":"sent"} ack), the local ladder (the direct row, the empty
eraId, the unknown account — every refusal byte-frozen), and the red
line (crash-in-flight answers SEND_OUTCOME_UNKNOWN across the restart,
exactly one crashing frame). Gated behind the full local gate suite
(fmt + clippy -D warnings + cargo test + release build), all green.

### 4.109 The envelope's server time lands in the store: `serverTimestamp` ingest (contract revision 1.128, 2026-10-12)

The engine's P15c 项5 batch started projecting `Envelope.serverTimestamp`
— the server-recorded receive time in epoch milliseconds (the official
event face's same-position key, `MessageReceiver.preload.ts:1178`) — on
every receive envelope, wire-additive: the proto-absent envelope still
projects 0 (the official `?? 0n` sentinel), the engine never invents and
never omits the key. This batch consumes it on the connector's receive
side as a store key: the normalized receive captures the value, and the
message row persists it — the 48h edit-window judgment input the engine
face documents, and the row's server-time key for the consumers that
follow (schema v22, `SCHEMA_VERSION` 21→22).

**The shape.** No new method, no wire face: the key is connector-
internal (`#[serde(skip)]` both on the normalized receive and the
message record — the desktop never sees it), captured at receive
(a positive value rides verbatim), folded at the sentinel (a projected
0 is the unknown server time and stores NULL — never the epoch, the
same honesty the `?? 0n` fallback encodes), absent on every row that
predates the key (the v22 `ALTER TABLE` migration adds the nullable
column in place; pre-1.128 rows, history-import rows, and outgoing
mirror rows without an envelope are never backfilled — the server time
a past envelope carried was only knowable from the envelope itself,
and inventing one would corrupt the edit-window judgment the key
feeds).

Tests: the engine unit pins the three capture faces (positive verbatim,
the 0 sentinel folding to None, the absent key to None); the store unit
pins the v22 upgrade (the pre-1.128 schema in place, the column added,
the version stamped) and the round-trip (a stamped row reads verbatim,
an unstamped row reads NULL); the integration face stays honest — the
key has no wire observation point on this connector (the consumer is
the engine-side edit window), so the store-level units carry the proof
and the receive path's behavior tests (the fixture envelopes already
exercise ingest) cover the lane. Gated behind the full local gate
suite (fmt + clippy -D warnings + cargo test + release build), all
green.

### 4.110 The notification profiles reach the host: `notificationProfiles.list` / `notificationProfiles.setRecords` (contract revision 1.129, 2026-10-12)

The engine's Storage 跨设备同步批 shipped the notification-profile face
pair — the official NotificationProfile record surface (manifest
NOTIFICATION_PROFILE=9, the official mergeNotificationProfileRecord
read/write faces, the desktop's notification-profile picker's own sync
pipe). This batch wires both into the host contract as
`notificationProfiles.list` / `notificationProfiles.setRecords`,
advertised through the new `notification-profiles` capability tag (the
1.34 discipline; one capability for the read and write halves, the
1.36/1.37 sync-face discipline). Until now the desktop could manage
chat folders (§4.78) but had no notification-profile surface; the
profile set had no host leg. The wiring template is the §4.78
chat-folder batch verbatim — same lanes, same gates, same
zero-local-footprint contract.

**The shape.** `notificationProfiles.list {accountId}` →
`{profiles: [...]}` — the engine's NotificationProfileJson entries
verbatim (the official mergeNotificationProfileRecord output face:
`id` (32-char lowercase hex), `name`, optional `emoji`, `color`,
`createdAtMs`, `allowAllCalls`, `allowAllMentions`, `allowedMembers`
(entries `{conversationId, kind: contact|group|legacyGroup}` — the
§4.78 recipient taxonomy), `scheduleEnabled`, the optional
`scheduleStartTime` / `scheduleEndTime` u32 windows (present only when
non-zero), `scheduleDaysEnabled` (the 1..=7 weekday array, always
present), and `deletedAtTimestampMs` present only when non-zero — the
tombstone key). Read lane, non-mutating, metrics `read`; the array is
capped at `MAX_NOTIFICATION_PROFILES` (64, the §4.78 cap verbatim) and
entries without a usable identity (a string `id`) drop (the §4.34
discipline). The engine's no-master-key / unknown-account face answers
the honest empty array.

`notificationProfiles.setRecords {accountId, records?, removeIds?}` →
the same `{profiles: [...]}` projection — the engine answers with the
post-write manifest, so the caller sees the resulting set without a
second read. Send lane, account-scoped mutating, metrics `send`, the
reaction class: a definite upstream error propagates, an unknown
outcome answers SEND_OUTCOME_UNKNOWN (retryable=false) and is never
retried. Local gates (all INVALID_REQUEST, retryable=false) mirror the
§4.78 ladder: at least one half non-empty (the
accounts.updateConfiguration rule), each half at most
`MAX_NOTIFICATION_PROFILE_BATCH` (64), every `records` entry an object
carrying a non-empty string `id` of at most
`MAX_NOTIFICATION_PROFILE_ID_CHARS` (128), every `removeIds` entry a
non-empty string of at most 128 chars — the identity face only; the
profile grammar (the 32-hex id word, the non-empty name, the
scheduleDaysEnabled 1..=7 domain) stays the engine's audit, never a
local re-derivation. The upstream frame carries only the non-empty half
(`records` / `removeIds` keys appear as needed), values verbatim.

**Zero local footprint, idempotency, and the error face** are the
§4.78 contract verbatim: no record content lands in the connector
store; the store's account id never rides the wire (resolved to the
upstream `account` at prepare, the §4.33 discipline); the idempotency
is the engine's own documented upload semantics (upsert by record id, a
non-zero `deletedAtTimestampMs` as tombstone, `removeIds` as the
official deleteKey face, a same-value rewrite as the engine's no-bump
no-op); and the error face is transparent — the engine's
STORAGE_WRITE_FAILED / STORAGE_READ_FAILED / STORAGE_UNAVAILABLE ride
the wire verbatim (already allowlisted, retryable=true) and the
engine's `-32603` folds to UPSTREAM_ERROR (retryable=true) through the
existing `into_api` face. The ladder order holds (shape gates →
resolve_account → engine): a bad record with an unknown account still
answers the shape, an unknown account with a clean frame answers
ACCOUNT_NOT_FOUND.

Tests: the service unit pins the prepare ladder (the batch caps on both
halves, the records identity face, the removeId bounds, the
at-least-one rule, the resolved non-empty-half frame with the store
account id never riding); the methods-table test pins the lane and
mutating classification and the capability list; the integration tests
(`tests/notification_profiles.rs`: `notification_profile_faces_pass_through_verbatim`
— the fixture's two profiles (a schedule-less one and a scheduled one
carrying emoji, windows, and an allowed member) read byte-faithfully, a
records-only write crossing verbatim and reading back the post-write
manifest with the `removeIds` key absent, a removeIds-only write reading
back the shrunk manifest with the `records` key absent;
`notification_profile_error_ladder_rides_the_wire` — the fixture's
failure markers landing STORAGE_WRITE_FAILED on the write,
STORAGE_READ_FAILED on the read, STORAGE_UNAVAILABLE on both, the
engine's grammar refusal degrading to UPSTREAM_ERROR;
`notification_profile_gates_answer_locally` — the at-least-one rule,
the non-object / `id`-less / empty-`id` / non-string-`id` entries, the
129-char ids on both halves, the 65-entry batches, the
shape-over-resolution and resolution ladder, and the engine-unknown
account degrading to UPSTREAM_ERROR;
`notification_profile_crash_answers_unknown_and_never_retries` — the
crash sentinel answering SEND_OUTCOME_UNKNOWN (retryable=false) with
exactly one dispatch frame in the log); plus the schema-consistency
bound pairs and the server handshake advertisement. Gated behind the
full local gate suite (fmt + clippy -D warnings + cargo test + release
build), all green.

### 4.111 The story distribution lists reach the host: `storyDistributionLists.list` / `storyDistributionLists.setRecords` (contract revision 1.130, 2026-10-12)

The engine's Stories 跨设备名单同步批 shipped the story-distribution-list
face pair — the official StoryDistributionList record surface (manifest
STORY_DISTRIBUTION_LIST=5, the official mergeStoryDistributionListRecord
read/write faces, the story-audience picker's own sync pipe). This batch
wires both into the host contract as `storyDistributionLists.list` /
`storyDistributionLists.setRecords`, advertised through the existing
`stories` tag widened in place (the 1.82 no-new-bit discipline — the
distribution lists are the story audience the §4.67 send faces
address). Until now the desktop could send stories and manage the
receive toggle but could not read or write its audience lists; the
audience half of the story life cycle had no host leg. The wiring
template is the §4.78 chat-folder batch verbatim — same lanes, same
gates, same zero-local-footprint contract — with the identity key the
official record carries: `identifier` (a uuid string), not `id`.

**The shape.** `storyDistributionLists.list {accountId}` →
`{lists: [...]}` — the engine's StoryDistributionListRecordJson entries
verbatim (the official mergeStoryDistributionListRecord output face:
`identifier` (uuid), `name` (a string that may be empty — the official
tombstone posture is a cleared name plus cleared members plus a set
`deletedAtTimestamp`), `recipientServiceIds` (service-id strings, ACI
uuid / PNI:uuid), `recipientServiceIdsBinary` (standard-base64 binaries,
16-byte ACI / 17-byte PNI), `deletedAtTimestamp` (always present, 0 =
live), `allowsReplies`, `isBlockList`). The two member faces project
independently — the official read face's binary-first/string-fallback
is a merge-cost decision the caller owns; the engine surfaces both
verbatim and the connector does not merge them. My Story is the
all-zero uuid projected like every other list, no special case. Read
lane, non-mutating, metrics `read`; the array is capped at
`MAX_STORY_DISTRIBUTION_LISTS` (64, the §4.78 cap verbatim) and entries
without a usable identity (a string `identifier`) drop (the §4.34
discipline).

`storyDistributionLists.setRecords {accountId, records?, removeIds?}` →
the same `{lists: [...]}` projection — the engine answers with the
post-write manifest. Send lane, account-scoped mutating, metrics
`send`, the reaction class: a definite upstream error propagates, an
unknown outcome answers SEND_OUTCOME_UNKNOWN (retryable=false) and is
never retried. Local gates (all INVALID_REQUEST, retryable=false)
mirror the §4.78 ladder over the `identifier` key: at least one half
non-empty, each half at most `MAX_STORY_LIST_BATCH` (64), every
`records` entry an object carrying a non-empty string `identifier` of
at most `MAX_STORY_LIST_ID_CHARS` (128), every `removeIds` entry a
non-empty string of at most 128 chars — the identity face only; the
record grammar (the uuid word, the service-id vocabularies both faces)
stays the engine's audit, never a local re-derivation. The upstream
frame carries only the non-empty half, values verbatim.

**Zero local footprint, idempotency, and the error face** are the
§4.78/§4.110 contract verbatim (no record content in the connector
store, the resolved upstream `account` the only frame addition, the
engine's own upload semantics the whole idempotency story, and the
transparent STORAGE_* / UPSTREAM_ERROR error faces). The ladder order
holds (shape gates → resolve_account → engine).

Tests: the service unit pins the prepare ladder (the batch caps, the
`identifier` identity face, the removeId bounds, the at-least-one rule,
the resolved non-empty-half frame); the methods-table test pins the
lane and mutating classification; the integration tests
(`tests/story_distribution_lists.rs`: `story_list_faces_pass_through_verbatim`
— My Story's all-zero uuid and the custom list's both member faces read
byte-faithfully, a records-only write crossing verbatim and reading
back the post-write manifest, a removeIds-only write reading back the
shrunk manifest;
`story_list_error_ladder_rides_the_wire` — the three markers plus the
engine's non-uuid grammar refusal degrading to UPSTREAM_ERROR;
`story_list_gates_answer_locally` — the at-least-one rule, the
identity face over `identifier`, the 129-char bounds, the 65-entry
batches, the ladder, the engine-unknown account;
`story_list_crash_answers_unknown_and_never_retries` — the crash
sentinel with exactly one dispatch frame); plus the
schema-consistency bound pairs and the server handshake advertisement.
Gated behind the full local gate suite (fmt + clippy -D warnings +
cargo test + release build), all green.

### 4.112 The storage merge/import and call-link faces reach the host: `storage.mergeContacts` / `storage.importGroups` / `storage.getCallLinks` / `storage.setCallLink` (contract revision 1.131, 2026-10-12)

The engine's window-D batches shipped the four storage-service faces the
desktop's first-run and call-link surfaces walk: the official
mergeContactRecord engine leg (`storage.mergeContacts` — the account's
ContactRecord set merged engine-side: identity keys into the identity
store, trust states, profile keys, contact rows; idempotent, a
same-key rerun a full no-op), the official mergeGroupV2Record engine
leg (`storage.importGroups` — the manifest GROUPV2 records landed
listGroups-ready, already-known groups skipped), and the official call
link record pair (`storage.getCallLinks` read leg /
`storage.setCallLink` write leg). This batch wires all four into the
host contract under one capability tag, `storage-sync` (the 1.34
discipline; one capability for the whole storage-sync family, the 1.53
appendix discipline). Until now the desktop had the record-list sync
faces (§4.78/§4.110/§4.111) but no merge/import trigger and no
call-link sync leg.

**The shapes.** `storage.mergeContacts {accountId}` → the engine's
camelCase summary verbatim — `{recordsRead, skipped, identitiesNew,
identitiesReplaced, profileKeysSet, contactsCreated,
trustStatesSynced}`; `storage.importGroups {accountId}` →
`{recordsRead, imported, alreadyKnown, failed}`; the connector adds no
numbers and renames no keys (a missing tally degrades to the struct
default keeping the projection total for additive engine growth; a
wrong-typed tally is the engine contract breaking and answers the
protocol error). Both are the contacts.sync class: read lane,
account-scoped mutating — the ingest lands engine state, so the
same-account mutex and delete barrier serialize it — metrics
`contacts`. `storage.getCallLinks {accountId}` → `{callLinks:
[entry...]}`, each entry the engine's `{rootKey (standard base64, 32
bytes), adminPasskey (base64 or null), deletedAtTimestampMs (u64 or
null)}` verbatim, capped at `MAX_CALL_LINK_SYNCS` (64), entries
without a usable `rootKey` dropping (the §4.34 discipline); read lane,
non-mutating. `storage.setCallLink {accountId, rootKey,
adminPasskey?, deletedAtTimestampMs?}` → the same `{callLinks: [...]}`
post-write projection; send lane, account-scoped mutating, metrics
`send`, the reaction class — an indeterminate outcome answers
SEND_OUTCOME_UNKNOWN (retryable=false) and is never retried.

**The local gates** (all INVALID_REQUEST, retryable=false, before any
upstream call): the rootKey identity face (non-empty, bounded 128
characters — the 32-byte base64 grammar stays the engine's definite
-32603, never a local re-derivation); the arms shape-gated
(adminPasskey a non-empty string ≤128, deletedAtTimestampMs a positive
integer — 0 is the engine's own no-action refusal mirrored); and the
exactly-one intent rule — both arms present is the official
isBadRemoteData same-position contradiction, neither is the empty
intent the official upload never writes. Only the provided arms ride
the upstream frame; a null arm folds to absent (the engine reads both
the same way). The key bytes ride the one call they were asked for and
are never logged or persisted connector-side.

**The error face** is the family contract verbatim: the engine's
STORAGE_WRITE_FAILED / STORAGE_READ_FAILED / STORAGE_UNAVAILABLE ride
the wire verbatim (retryable=true), the engine's `-32603` folds to
UPSTREAM_ERROR (retryable=true), and the ladder order holds (shape
gates → resolve_account → engine).

Tests: the service unit pins the setCallLink intent ladder (the
exactly-one rule both ways, the zero-timestamp refusal, the rootKey
identity face, the arms-only-when-present frame); the methods-table
test pins the four lanes and the mutating classifications and the
capability list; the integration tests (`tests/storage_sync.rs`:
`storage_ingest_summaries_pass_through_verbatim` — both summaries key
for key, the frames carrying exactly the resolved account;
`storage_call_link_faces_round_trip` — the read pinning the live and
tombstoned fixture links verbatim, the active-arm and delete-arm
writes each forwarding exactly their arms and reading back the
post-write state, the five-way local refusal ladder with a frozen
dispatch log, and the unknown-account faces;
`storage_error_ladder_rides_the_wire` — the three markers across the
four faces plus the engine-unknown account degrading to
UPSTREAM_ERROR;
`storage_set_call_link_crash_answers_unknown_and_never_retries` — the
crash sentinel with exactly one dispatch frame); plus the
schema-consistency bound pairs and the server handshake advertisement.
Gated behind the full local gate suite (fmt + clippy -D warnings +
cargo test + release build), all green.

### 4.113 The conversation flags have no inbound leg: the remote-storage-record face the engine does not project (verification record, 2026-10-12)

The §4.97 conversation-state family (`conversations.getState` /
`conversations.setState` over the mutedUntilTimestamp/archived/
markedUnread bits) is desktop-local-write only: the connector's
`setState` rides the engine's outbound write leg — the official
Storage Service ContactRecord/GroupRecord flag flip
(`apply_conversation_state`, kt-signal-engine `src/service/
storage_sync.rs`, the proto bits 11/12/13) — and lands the change on
the cloud record. This section records the verification of the other
direction: when a *different* linked device flips the same bits on the
Storage Service, does anything push the change to this connector? The
answer, verified against the engine's current notification and
storage-merge surfaces, is no — the engine has no inbound face here,
and the connector wires nothing because there is nothing to wire.

**The evidence.** The engine's complete notification vocabulary is
four methods — `accountUnbound` (§4.94), `receive` (the envelope
stream), `receive.clockskew`, and `receive.emptied` — and no storage
notification rides any of them: the `receive` envelope keys the engine
projects (`contactProfileUpdated`, `groupTerminated`, `identityChanged`,
`storyMessage`, the sync faces) are message-decryption products, never
storage-record diffs. The engine runs no Storage Service manifest
subscription at all — its storage faces are connector-invoked
request/response calls (read on demand, write on intent), so remote
manifest revisions never arrive while idle; the only code paths the
word "manifest" names are sticker-pack CDN manifests. And the inbound
record-application path (`src/service/storage_merge.rs`, the
§4.112 merge/import ingest family) lands remote records into the
engine's own database and answers merge summaries — it emits no
per-conversation notification and the three flag bits are not part of
any summary it answers.

**The consequence.** A remote flag edit (another device mutes,
archives, or marks-unread a conversation) reaches this connector only
if something pulls the storage records that carry the bits — and no
current face does: the record-list sync faces (§4.78 chatFolders,
§4.110 notificationProfiles, §4.111 storyDistributionLists) carry
their own record families, not the conversation-state bits. The
desktop's conversation list therefore does not converge on
other-device flag edits; the §4.97 faces remain write-out,
read-local-store.

**The disposition.** Recorded as an engine projection gap (the inbound
half of the conversation-state family). No connector code, no schema
change, no capability tag, no contract revision. When the engine ships
the inbound face — a `receive`-lane notification per remote
conversation-state change, or a pull face carrying the three bits —
the §4.97 section gains its inbound leg through the same
ControlReceive interception family the other record-sync faces use
(the §4.81–§4.87 pattern), and this section's gap closes.

### 4.114 The four engine notifications wait at the door: the emptied / clockskew / groupTerminated / contactProfileUpdated forwarding batch (verification record, 2026-10-12)

The engine's window-D/G batches shipped four account-scoped stream
notifications the connector does not consume yet. This section records
the verified emit faces and the current connector-side consumption
(today: all four dropped) so the forwarding batch can wire them
without re-deriving the engine side. No connector code, no schema
change, no capability tag, no contract revision this round.

**The emit faces** (kt-signal-engine `src/service/mod.rs`, verified
2026-10-12):

- `receive.emptied` `{account}` (line ~4046) — the history-queue
  drained (`Received::QueueEmpty`, the official
  ApiEmptyQueue→EmptyEvent peer); emitted once per drain at :4766.
  Standalone JSON-RPC method.
- `receive.clockskew` `{account, skewMs, serverTimestamp}` (line
  ~4118) — the local clock sits more than the official 24h threshold
  from the server stamp; edge-triggered (one alert per threshold
  crossing, sustained skew silent). Standalone method.
- `contactProfileUpdated` `{aci}` (line ~5839) — a peer's
  profileKeyUpdate batch landed a new profile key in the engine's
  store; rides the `receive` envelope as a key (the
  `identityChanged` shape family).
- `groupTerminated` `{groupId, terminator?, timestamp}` (line ~5687) —
  a decrypted group-change names the group terminated; the engine
  replaces the whole envelope with the event (change-only projection,
  `receive_projects_group_terminated_change_only`).

**The current consumption.** The two standalone methods fall into the
`handle_upstream_line` default arm and answer
`ProtocolWarning::unknownNotification` — warned, dropped. The two
envelope keys match no `control_routing` arm and land in
`normalize_receive_fields`'s terminal default as `direction: "skip"`
receives — silently dropped by the service layer. Nothing of the four
reaches a host frame today.

**The wiring plan** (the batch this section front-runs):

1. The two envelope keys join the `control_routing` ladder as
   `ControlReceive` variants — `groupTerminated` routes by its own
   groupId (the conversation-less transient family like `story`,
   though the groupId names its subject), `contactProfileUpdated` is
   conversation-less (the `aci` names its subject); the service layer
   forwards both as transient host events through the §4.67
   `StoryReceived` shape (no row, nothing persisted).
2. The two standalone methods need the account-scoped transient-event
   lane that does not exist yet: `EngineEvent` today carries only
   group-level facts, and the one account-scoped precedent
   (`AccountUnauthorized`) is consumed by the supervisor's unlink
   watcher, not projected to a host frame. The lane: a new attributed
   event variant carrying the signal account, resolved to the
   connector's opaque `accountId` through the store (the
   `accountUnbound` attribution discipline — a malformed or unmatchable
   account stays unattributed and warn-only), then the host frame.
3. The host frames: transient events on the shared host event stream
   (`receive.emptied` / `receive.clockSkew` /
   `contact.profileUpdated` / `group.terminated` provisional names,
   §4.74 naming family), schema event definitions with the
   `$defs` shapes, the fixture branches emitting all four (the
   crash-sentry discipline does not apply — notifications are not
   request/response), and the four-lane test set (pass-through, error
   face, local gate n/a — the events carry no request, crash n/a —
   the engine staying alive is the assertion).

**The disposition.** Deferred as a scoped batch, not a gap: the engine
side is complete and stable (the shapes above), the connector side is
planned to the arm level. The next storage/notification batch picks
this section up; until then the four notifications keep their current
drop behavior, which is safe (nothing is lost that the desktop waits
for — queue-drain and skew are advisory; profile-key and
group-termination refreshes ride the next full pull).

### 4.115 The plaintext chat export reaches the host: `backups.exportPlaintext` (contract revision 1.132, 2026-10-12)

The S1 plaintext-export batch (方案 A「明文不过引擎」, desktop施工
图 `signal-plaintext-export-plan.md`): the official desktop has no
plaintext export at all (Preferences → Chats carries the entry with no
data face — greenfield parity), and the §4.46 encrypted export covers
restore, not data portability. This face projects the connector's own
SQLite store into a plaintext JSONL artifact the desktop streams home
through the unchanged §4.46 file three-chain. **The engine takes zero
part in the data path** (red line 1): no network request, no new
upstream face — the engine process neither sees nor serves a single
byte of the export, and the artifact never lands in the engine's data
tree (it lives under the connector's own state directory,
`plaintext-exports/<accountId>/`). The one engine touch on the whole
face is the start slot's best-effort read-only `backup.exportStatus`
poll (below) — a control-plane registry read, content-free, carrying
no export byte in either direction; the build itself never calls the
engine. `backups.export`'s Android backup
semantics are unchanged — not one gate, code path, or artifact of the
§4.46 machine moves.

**The face.** `backups.exportPlaintext {accountId}` →
`{status: "started"}`; send lane, account-scoped mutating (the
`backups.export` class verbatim: write-lane mutex and delete barrier),
metrics `accounts`, capability `backup-export` widened in place (the
1.87 upload precedent — no new bit). The build runs connector-side on
the blocking pool behind a connector-local per-account task registry:
one task slot per account, shared with the encrypted export in the
start direction — a second start while a plaintext build runs answers
`BACKUP_PLAINTEXT_ALREADY_RUNNING` (retryable=false; poll
`backups.exportStatus` instead of retrying, the engine's
BACKUP_EXPORT_ALREADY_RUNNING semantics mirrored), and a start while
the engine's encrypted export task reports `running` answers
`BACKUP_EXPORT_ALREADY_RUNNING` (the engine's own code riding the
connector's start face). The engine slot check is best-effort: an
engine that is down or answers indeterminately cannot hold a task
(its registry is process memory), so the plaintext start proceeds —
the two builds share no file, no lock, no state. The reverse
direction is deliberately not gated this batch: `backups.export`
keeps the engine's registry as its sole slot authority (Android
backup semantics zero change), and a plaintext build — seconds-scale
bounded local IO writing a disjoint file — cannot corrupt the
engine's export.

**The artifact** (`plaintext-exports/<accountId>/plaintext-export-
<yyyy-MM-dd-HH-mm-ss>.jsonl`, the §4.46 snapshot-name timestamp shape):
UTF-8, one JSON object per line, reference-before-referencing order —
`account` (the account's own address spelling plus `givenName` when
known), `contact` rows (the §4.46 identity projection verbatim:
deterministic ids in canonical-key order, `aci`/`e164`/`givenName`,
roster members merged, self never a contact row), `group` rows
(`masterKey` hex plus the conversation title when known), `chat` rows
(`kind` + `recipient`, so an independent parser never has to guess
which id space a recipient references), then `message` rows per chat
in store order. A message line carries the row's store id (the stable
join key), the `chat` line it rides (the chat line's own `id`), the
`direction`, the author resolved through the §4.46 sender
reverse map (0 = self), `sentAt`, `receivedAt?`, `status` (the receipt
stamp the row carries), `deliveredAt?`/`readAt?`/`viewedAt?`,
`editedAt?`, the body, and the v1 projection faces the owner's 带面
admitted: `quote` (NormalizedQuote verbatim — `id`/`text` bounded
preview/`attachments` metadata — plus `messageId` when the store
resolved the target, the join key to the quoted row's own line; the
author resolves through the same sender map, and an unresolvable
author rides `authorRaw` verbatim rather than dangling), `sticker`
(NormalizedSticker metadata verbatim), `attachments` (the
NormalizedAttachment metadata entries verbatim — the placeholder face;
attachment BYTES are the phase-two 「仅本地在盘+覆盖率报告」 switch
and never ship in v1), and `reactions` (the message_events rows
targeting the message — emoji, resolved author, `removed`,
`reactedAt` — removed rows included, history stays auditable). The
row-level gates are §4.46's verbatim: system rows, remote-deleted and
admin-deleted tombstones, and unconfirmed outgoing rows never
masquerade as delivered history; a truncated preview body never
becomes export text (`skippedBodyTruncated`); a row with neither body
nor quote nor sticker nor attachments carries no conversational
content and counts `skippedRows`. Skeleton peers (no ACI/E164 shape)
stay unprojectable and count `skippedSkeletonChats`.

**The caps and the counters** are the §4.46 machine reused, not
re-lit: per-line 1 MiB, 4,000,000 lines, 2 GiB total, 128 KiB body —
the build fails closed mid-write and removes the partial file
(tmp-name write plus atomic rename, red line 4: an interrupted export
never leaves a partial product addressable). The per-cause
`SkipCounters` block merges into the status answer of a plaintext task
exactly like the encrypted build's: `skippedQuotes`/`skippedStickers`
count projection failures (the faces project — a malformed stored JSON
arm counts and the line rides on without it); `skippedReactions`
counts every reaction row that earned no line face — a malformed arm,
a target row the row gates skipped, and the fuse-cut tail of the
per-conversation reaction read (a runaway `message_events` table has
no retention sweep, so the read carries its own fuse) — never a
silent drop; and `skippedAttachments` keeps its byte-honest meaning:
one per row whose attachment descriptors exist while their bytes did
not ship (v1 never carries bytes).

**The status face** (`backups.exportStatus`, additive — old readers
ignore the new key): the answer gains the optional `kind` field,
`"backup"` when the engine's encrypted task owns the latest run,
`"plaintext_jsonl"` when the connector-local plaintext task is the
latest started (startedAtMs wins the comparison; a plaintext task
newer than the engine's answer projects the plaintext registry's own
state, startedAtMs/finishedAtMs/lines/bytes/error class — content-free
classes and counters only — with `artifactPresent: false`, because no
engine artifact exists on this path). `kind` stays absent when no task has
ever run (honest absence, never inferred). The disk truth is
symmetric: snapshot listing stays the engine family's, and
`backups.exportFile.open` gains the additive third `file` value
`plaintext_jsonl` addressing the newest plaintext artifact on disk —
the three-chain mechanics (handles, sequential chunks, TTL, per-account
ceiling) are untouched. Red line 3 holds across the whole face: bodies,
quote previews, file names, and captions never enter logs or error
messages — counters and error classes only. Account deletion removes
the account's plaintext artifact directory and task record beside the
existing handle cleanup, so an export product never outlives its
account session.

**Error face.** `ACCOUNT_NOT_FOUND` (unknown accountId),
`INVALID_REQUEST` (malformed params), `BACKUP_PLAINTEXT_ALREADY_RUNNING`
and `BACKUP_EXPORT_ALREADY_RUNNING` (retryable=false — poll, never
retry), `CAPABILITY_UNAVAILABLE` (the launcher did not arm the
backup-export family), `STORAGE_READ_FAILED` (a store read failed —
the artifact is a store projection), `INVALID_REQUEST` when the
connector's own projection exceeds a cap (fail-closed before the
partial file would exist), `STORAGE_WRITE_FAILED` (the artifact could
not be written). The engine is never called, so no engine error code
can surface from this face.

### 4.116 The S2a measurement layer reaches the connector: the ingest transaction instrumentation + the doctor `perfMetrics` face (contract revision 1.133, 2026-10-12)

The connector slice of the S2a instrumentation layer (the desktop registry
`signal-runtime-followup-projects.md` §附四②: measure-only, baseline first,
optimization is a separate batch with owner sign-off). Three registered
leftovers; two land here, one is verified out of this repository's reach.

**1. The per-envelope ingest transaction is measured** — the registered
largest-bottleneck candidate (a 30-envelope burst = 30 transactions; the
engine side already batched its own store window, the connector's local
store leg is an independent face). The `receive_persistence_loop` attempt
is now wrapped with `Instant` timing and counters in
`src/perf_metrics.rs` (the connector's own bounded registry, the engine
slice's shape: closed word list, unknown keys drop and count, BTreeMap
deterministic snapshot order, honest absence, microsecond
`totalUs/maxUs/lastUs`): `receive_ingest` (wall time per transaction
attempt, failed attempts included), `receive_ingest_ok` /
`receive_ingest_err` (attempts = ok + err; each err sleeps and re-attempts),
and `receive_ingest_events` (host events the landed transactions produced,
summed — the realized batch size; the mean is events / ok). Measure-only by
construction: the call, its arguments, and the control flow around it are
byte-identical; the layer holds counts and durations only — zero message
content, zero addresses, zero key material, per the perfMetrics red line.

**2. The engine snapshot passes through a whitelist, not verbatim** — the
doctor's per-group `engine` face gains the additive `perfMetrics` probe
(same reachability gate, same read-only 3-second diagnostic budget, same
degradation family as the `version` / `engine.protocolCapabilities`
faces): the engine's read-only `engine.perfMetrics` answer (engine slice
`38238b3`, a closed 31-key vocabulary — 12 timing series + 19 counters)
projected through the connector's own pinned copy of that vocabulary
(`ENGINE_TIMING_KEYS` / `ENGINE_COUNTER_KEYS`). The projection is
deliberately **not** the verbatim passthrough those two faces use: a
future engine may grow its metric vocabulary, and this face never forwards
an unbounded key space — unknown top-level keys, unknown
`timings`/`counters` entries, and malformed entries drop and count into
the face's always-present `connectorDroppedKeys` (a meaningful zero, so a
clean projection is distinguishable from a lossy one without diffing key
sets); `uptimeMs` / `unknownKeyDropped` pass through only as `u64`;
whitelisted well-formed entries keep the engine's own series object
verbatim. Face shape: `present: true` with the filtered projection, or
`present: false` with the same reason strings the other probes use (a
stock signal-cli engine, or an engine predating `engine.perfMetrics`,
answers the upstream-error class — the honest degradation, a diagnostic
never fails as a whole because one engine face could not answer). The
connector-side dropped keys also accumulate into the local registry's
`engine_snapshot_dropped` counter. Route record: this rides the **existing
doctor payload as an additive key** rather than a new connector method —
one wire surface, one advertisement story, zero new methods or error
codes; pre-1.133 hosts read byte-identical doctor answers without the
key. Schema unchanged: the doctor result has never been schema-pinned
(the JSON schema carries only the method name in the request enum), so
there is nothing to sync.

**3. The vendored batch-window three-phase split is not connector-side
(verification record)** — the S2a engine record's "vendored 批窗内部三相
未打点" was verified against both trees before any code moved: the batch
window is the **engine repository's** vendored presage receive pipeline
(75 ms / 30-window fill → decrypt → batch persist,
`kt-signal-engine/vendor/presage`, per the engine's own
`docs/jsonrpc-compat.md` record), not a connector machine. The connector
has no presage dependency and no batch window in its receive leg — the leg
is per-envelope by construction (exactly what measurement 1 now proves
with numbers). Instrumenting the three phases means touching the engine's
vendor tree under the engine repo's `vendor/README.md` discipline — an
engine-side batch, and the engine repository was read-only for this batch.
The connector-side proxies are already in place: measurement 1 covers the
connector leg, and the passthrough carries the engine's
`receive_envelope` series, which the engine record notes already includes
the batch window's end-to-end latency. Naming record: the three phases are
窗填充/解密/批量落库 (window fill / decrypt / batch persist), not the
攒窗/落库/提交 shorthand the leftover list carried.

**Same-batch non-wire repairs** (no contract surface): the Status header
revision chain, stale since 1.121, now carries the 1.122-1.132 index (each
revision's own record has always lived in its §4.10x section); the §2
process diagram and README now state the kt-engine production reality
(ADR 0003 demoted the pinned signal-cli runtime to the
development/comparison fallback); the ADR directory's 0002 collision is
resolved — the media-ingest ADR (2026-09-23, the later of the two) is
renumbered 0004 with every media-facing reference updated and every
namespace/lock reference (the multiple-instances ADR, 2026-09-06) left on
0002; the 1.108-era test totals (640 → 641, header and §4.89) carry the
pre-recalibration bracket; `runtime.doctor`'s credentials-face comments
corrected §4.79 → §4.82. The parity matrix (the desktop repository's
`signal-official-parity-matrix.md`) carries no doctor-related rows at all
— it is a UI-feature alignment matrix, so no row can drift against this
face; recorded rather than edited. The desktop registry's connector-slice
leftover list is backfilled by the desktop batch, not here.

Tests: nine unit tests over the new registry and the engine-snapshot
filter (series aggregation, unknown-key drop counting, word-list
closedness and its disjointness from the engine allowlist, deterministic
key order, whitelist passthrough with drop counting, honest absence for
malformed scalars, fail-closed on a non-object answer, the meaningful
zero) plus the `runtime.doctor` integration test extends the engine-face
assertions with the `perfMetrics` face (present + projected + drop count
against the fake engine's echo). Gates: tests 700 → 709 (lib 523 → 532),
all green; fmt / clippy / release build clean.

### 4.117 The four engine notifications (and the conversation-state fifth) reach the host: the account-domain transient lane + the two envelope-key forwards (contract revision 1.134, 2026-10-12)

The §4.114 front-run batch lands: the engine's four account-scoped stream
notifications the connector used to drop, joined by the fifth the engine
shipped meanwhile (`receive.conversationState`, engine `e60d673`), all
reach the host as transient events. Wire additive-only: five new host
event names, no new method, no new error code, no capability change, no
store version change. Every event is transient — no row, no self-heal,
nothing persisted; a lost notification is the lost official semantics of
that one notification, exactly like the official client losing one it
received while offline.

**The engine faces (verified 2026-10-12, `kt-signal-engine` main @
`e60d673`).** The §4.114 record held for the four; the fifth needed its
own reconnaissance, and it changed the shape assumption:

- `receive.emptied` `{account}` — standalone JSON-RPC method, the
  history-queue drained (the official ApiEmptyQueue→EmptyEvent peer),
  once per drain.
- `receive.clockskew` `{account, skewMs, serverTimestamp}` — standalone
  method, edge-triggered past the official 24h threshold; `skewMs` is
  signed (local − server, positive = the local clock fast).
- `groupTerminated` `{groupId, terminator?, timestamp}` — a receive
  envelope key (the whole envelope replaced, the change-only projection);
  a decrypted group change named the group terminated.
- `contactProfileUpdated` `{aci}` — a receive envelope key (the
  `identityChanged` shape family, `sourceUuid` equal to the aci); a
  peer's profileKeyUpdate batch landed a new profile key.
- `receive.conversationState` `{account, conversations: [{conversationId,
  kind, mutedUntilTimestamp, archived, markedUnread}], overflow}` — a
  **standalone method**, not an envelope key: the engine's F-link batch
  (e60d673) put the three-flag inbound flips on the same account-domain
  method lane as emptied/clockskew (词表第五词), differenced against a
  per-account in-memory book (8192 conversations/account cap,
  first-observation seeds silently, value-unchanged never notifies),
  one batch per storage merge with a 256-entry batch cap and an
  `overflow: true` tail marker; `kind` is `contact | group |
  legacyGroup`. The connector forwards the array verbatim.

**The wiring.** Two lanes, one forwarding posture:

1. *The envelope keys join the `control_routing` ladder* as
   `ControlReceive` variants. `groupTerminated` routes by its own
   groupId (the routing address carries the subject) and forwards
   through the conversation-less transient family — the service layer
   intercepts before the conversation ladder on purpose, because a
   terminated group's row may already be gone and the event is the
   desktop's termination fact, not a row anchor (a deliberate
   difference from the groupUpdated/groupChanged faces, which converge
   rows and die on unknown groups). `contactProfileUpdated` is
   conversation-less (the aci names its subject). Both fail closed on
   their guaranteed subject (no groupId / no aci = protocol error =
   the plain skip); optional faces (terminator/timestamp) degrade to
   absence, never to invented values (宁缺勿错).
2. *The standalone methods get the account-domain transient-event lane*
   (new): `EngineEvent` today carried only group-level facts, so the
   batch adds three attributed variants
   (`ReceiveEmptied` / `ReceiveClockSkew` / `ConversationState`)
   carrying the signal account, emitted from `handle_upstream_line`
   arms ahead of the unknownNotification default. The supervisor's
   engine-event watcher (the accountUnbound consumer, now the lane's
   consumer too) resolves the account through the store into the
   connector's opaque `accountId` and sends the host frame.
   **Attribution discipline** (the accountUnbound precedent): a
   malformed or empty account in the engine notification falls back to
   the unknownNotification warning at the parse arm; an account that
   matches no bound connector account drops at the service arm
   warn-only with a content-free warning and a perf counter —
   attribution failure is never silently swallowed into a claimed
   forward. The two envelope-key faces ride the receive pipeline's
   existing account resolution (the shared ingest ladder), which keeps
   its silent-skip discipline unchanged — nothing is claimed as
   forwarded that was not.
3. *F rides the same lane*: `receive.conversationState` parses the
   guaranteed faces only — `account` (attribution), `conversations`
   (non-empty array, then **carried and forwarded verbatim**: no entry
   parsing, no filtering, no trimming, unnamed sibling keys ride — the
   owner red line: a passthrough structure error corrupts every
   downstream consumer), `overflow` (boolean). A non-array or empty
   conversations face, a non-boolean overflow, or a missing skewMs is
   a protocol error and answers the unknownNotification warning, never
   a guessed forward. The engine's own caps govern the wire; the
   connector adds none.

**The host frames** (the shared host event stream, all transient):

| engine face | host frame | frame data |
|---|---|---|
| `receive.emptied` | `receive.emptied` | `{accountId}` |
| `receive.clockskew` | `receive.clockSkew` | `{accountId, skewMs, serverTimestamp?}` |
| `groupTerminated` | `group.terminated` | `{accountId, groupId, terminator?, timestamp?}` |
| `contactProfileUpdated` | `contact.profileUpdated` | `{accountId, aci, timestamp?}` |
| `receive.conversationState` | `conversation.stateChanged` | `{accountId, conversations, overflow}` |

Naming record (deviations from the §4.114 provisional names, all
cosmetic and documented here): the four provisional names stand as
pinned (`receive.emptied`, `receive.clockSkew` — the host frame
normalizes the engine's lowercase `clockskew` method name to the
frame vocabulary's camelCase, `contact.profileUpdated`,
`group.terminated` — the group.changed family sibling). The fifth was
left open pending reconnaissance; the verified face being an
account-domain method carrying a per-conversation batch, the frame is
`conversation.stateChanged` — the `conversation.*` family
(conversation.changed / conversation.typing) with the stateChanged
verb the proxyGroup.stateChanged sibling already uses; `receive.*`
stays the engine-method-mirroring pair above rather than growing a
third member that is really about conversations.

The schema grows the five `$defs/event` enum members and their data
branches (the conversations entries deliberately typed `object` and
open — pinning their internals would promise a connector-side
whitelist the F red line forbids). The perf layer (the §4.116 closed
word list) grows exactly two counters: `account_transient_forwarded`
and `account_transient_unattributed` — the lane's honest face: the
forward count and the warn-only drop count, content-free.

Tests: the engine parse arms (the three standalone notifications into
their attributed events with the signed skew and the verbatim array;
the malformed family — missing/empty account, missing/non-numeric
skew, non-array or empty conversations, non-boolean overflow — into
the unknownNotification warning; the two envelope keys into their
control routings with the fail-closed subject gate); the service
forward arms (the terminated group forwards without a local
conversation and persists nothing; the profile notify rides the
envelope timestamp; the lane resolves the bound account, forwards the
array verbatim, and drops the unbound account with both perf
counters); the host wire frames (all five, absent-optional identities
absent, the array verbatim); and the end-to-end harness over the real
engine process: the fixture grows a raw-notification marker
(`.fixture-emit-notifications.json`, full JSON-RPC objects verbatim)
beside the envelope marker, and one test walks the lanes — the three
standalone frames with resolved accountId, the 257-entry batch past
the engine's cap forwarded whole with the overflow tail marker, the
two envelope-key frames through the extra-receives channel (read
beside the flush request's reply with a collecting read — the
forwarded frames race that reply, and a reply-only read eats them),
the unbound-account non-forward, and the malformed face surfacing as
runtime.protocolWarning. The §4.114 four-lane set: pass-through and
the error face are exercised; local gate is n/a (the events carry no
request) and crash is n/a (the engine staying alive is the
assertion). Tests 709 → 717 (lib 532 → 539).

### 4.118 The polls arrive live: the engine's Q3 receive faces join the projection chain (contract revision 1.135, 2026-10-12)

The §4.106 batch gave the desktop the poll send trio; until now a poll
still only reached the host through the §4.38 archive import — a poll
created after the link surfaced only at the next import, and votes and
terminations never arrived live at all. This batch closes the receive
side: the engine's six-question-batch Q3 poll faces (verified
2026-10-12 against `kt-signal-engine` `docs/jsonrpc-compat.md` and the
official `processDataMessage.preload.ts:395-441` — behavior reference,
zero code copied) ride the live receive chain. Wire additive-only: the
`poll` row key has existed since 1.81 and the live chain lands the
identical face, so the desktop's history-shape consumer (engine
`4b6fa633` lineage) is untouched — no new method, error, capability, or
schema JSON, and zero send-side coupling (the §4.106 shapes stay
byte-still).

**The three faces and where they land.**

- `dataMessage.poll {question, options, allowMultiple}` — the create
  is a **message row**, not a control: the bodyless dataMessage ladder
  grows one arm (after the text/attachment ladders, beside the
  §4.41/§4.44 system-row arms) landing the outgoing timeline's poll
  bubble. The parse is structural (the official bare-poll posture:
  `question ?? ''`, `options ?? []`, `Boolean(allowMultiple)`):
  option entries keep their position so vote indexes stay addressable
  (a mistyped label rides empty, never shifts the list), a non-object
  face is no face and the envelope keeps the bodyless skip. The face
  is the §4.38 import's exact `NormalizedPoll` — `terminated` false,
  no votes — so the two chains render identically; the caps are the
  import family's values verbatim (question/option 256 chars, 32
  options), so a poll renders the same no matter which chain landed it.
- `dataMessage.pollVote {targetAuthor, targetSentTimestamp,
  optionIndexes, voteCount}` — a **control receive** (the
  reaction/remoteDelete family posture): it folds onto the poll row it
  addresses and never mints a row. The guaranteed faces gate
  fail-closed (a missing/blank targetAuthor, a 0 or absent target
  timestamp, a non-array optionIndexes → the plain skip); the
  selection dedupes preserving poll order (the official
  replace-on-vote set semantics) and caps at the §4.38 bound (32);
  `voteCount` has no desktop face and does not transport — the §4.38
  projection gap verbatim. The fold is the official vote semantics:
  the voter's full selection replaces their entry in place, an empty
  selection is the clear-vote action, an index past the poll's own
  option bound filters out (a selection that filters to nothing never
  lands — a hostile selection cannot erase a legitimate vote under the
  clear shape), and the §4.38 64-voter cap holds. The voter is never
  carried on the wire face: the service layer attributes the envelope
  source (a peer's vote) or the account itself (the multi-device sent
  echo) — the same identity family the import fold's `voterId` carries.
- `dataMessage.pollTerminate {targetSentTimestamp}` — a **control
  receive** flipping the addressed row's `terminated` bit, idempotent
  (a replay is an honest no-op). The proto carries no author: the
  official terminate send is the poll author's own dataMessage and the
  official consumer anchors the poll creator by the envelope sender
  (`onPollTerminate`: "the fromConversationId must be the poll
  creator"), so the connector anchors the same way — the account on
  the sent echo, the envelope source on a peer's terminate; a peer
  terminate with no attributable source drops (宁缺勿错).

**Row addressing.** The fold resolves the poll row through the §4.6
(sentAt, author-pair) identity — the receive-dedupe/edit-arm family —
trying the author candidates in order: the account's own outgoing
mirror when the anchor names the account (the row's sender projection
is the account id / `self` legacy pair), the sender hash built
straight off the anchor string (the engine's `targetAuthor` and the
create envelope's `source` flip between E.164 and ACI through the same
contacts state, so the direct hash matches whichever form landed the
row), and — for an ACI-shaped anchor — the §4.37 `peer_identities`
reconciliation for a row that landed under the number form. The store
half fails closed everywhere: an absent row, a two-rows-at-one-identity
ambiguity (the `message_by_signal_identity` refusal), a row without a
rich column or without a poll face, and `system`-status rows all
answer no-fold and mutate nothing — a vote can never invent a poll
face on a foreign row. A fold whose row has not landed buffers
§4.91-style, one entry per candidate keyed to each candidate's sender
projection, and the create's landing flush applies it — the event
stream reads land-then-converge (the create's upsert, then the folded
upsert), bounded by the shared TTL/capacity table.

**The live/history dedupe and the echo posture.** The live chain only
accelerates arrival; the archive remains the bulk truth and the two
never duplicate. A history refresh after a live landing skips at
`insert_history_batch`'s existing identity dedupe (the number/ACI
duality: both candidate sender hashes qualify as "already stored") —
no second row, the summary never regresses. The shared boundary rides
unchanged: the skip is row-level, so votes archived in the backup do
not merge into a live-landed row — the same discipline every rich face
already rides (live state is never regressed by a backfill). The
self-echo posture (自发回声归属): the account's own group-send
transcript carries the full dataMessage, so the own create lands the
outgoing mirror row (never an unread, the self send's own timestamp)
and the own vote/terminate echoes attribute the account itself — the
account's own vote renders as the account's, never as someone else's
event.

The 1.81-era "no live-receive counterpart" comments on the poll
face (the `NormalizedRich.poll`/`NormalizedPoll` docs) are repaired to
describe both chains.

Tests: the engine parse arms (the create landing the history-shape row
with the wire face byte-equal to the import chain's keys; the
degradation family — structural defaults, positional option
integrity, the non-object no-face; the vote/terminate control routings
with the dedupe-keeps-poll-order selection and the eight-face
fail-closed ladder; the three sent echoes landing outgoing); the
service arms (create-then-vote-then-terminate converging one row with
no minted control rows; the vote-before-create buffering and folding
on landing; the own echo landing the outgoing mirror with the account
voter and no unread; the non-poll-row refusal); the store arms (the
replace/clear/out-of-range semantics, the fail-closed
missing/ambiguous/unpollable/system ladder with the incoming/outgoing
leg disambiguation, the 64-voter cap with the capped-voter
replace-through); and one end-to-end harness test over the real engine
process (create lands the history shape, the vote+terminate emission
converges the same row on `messages.list`, one row total). Tests 717 →
729 (lib 539 → 550), all green.

### 4.119 The outbound receipt resupply stopgap: the seven-day recency window, the viewed-face selection filter, and the supervisor failure debounce (field-incident record, 2026-10-12; no contract revision — wire unchanged)

The field evidence (2026-10-12 production trace): the desktop fires receipt sends on every
conversation open, focus, foreground return, and new-message arrival, and the connector's
absent-mode receipt selection (`incoming_rows_for_receipts`, the no-`messageIds` arm of
`messages.markRead`/`messages.markViewed`, §4.29) took the conversation's OLDEST 512 incoming
rows with no filter at all. Every trigger therefore re-sent the same stale timestamps as
read/viewed receipts; the upstream rejects receipt timestamps that old, so the sends could
never succeed — a de-facto infinite resupply (the trace shows 47 receipt sends with 22
upstream rejections). The official desktop never behaves this way: `sendReceipts.preload.ts`
sends only the INCREMENTAL "newly changed since the last mark" set — an already-read message
is never re-sent, and messages whose receipt errored are explicitly excluded so a failing
receipt cannot loop.

Three connector-internal changes close the loop. No wire key, method, error code, or
capability changes; no store schema move; the contract revision stays 1.135.

1. **Viewed-face selection filter.** `incoming_rows_for_receipts` gains a face parameter;
   the viewed faces (peer `sendViewedReceipt` and self-sync `markViewed`, which share the
   selection) select only rows with `viewed_at IS NULL`. The §4.77 local-first first stamp
   becomes the resupply boundary: a row is played upstream at most once per stamp — the
   already-stamped rows drop out of every later absent selection, which is the official
   never-re-send-a-read-message posture. The read faces select without that filter (the
   store carries no read state — the desktop owns its unread state, §4.29).
2. **Seven-day recency window.** All four receipt faces' absent mode selects only rows
   with `sent_at > now − 7 days`; older rows drop out of the selection (zero eligible rows
   is the trivial `sent` no-op). The upstream rejects stale timestamps, so a resupply past
   that age is protocol-meaningless. The explicit-`messageIds` mode is untouched: the
   desktop naming ids is itself the incremental act the official client performs.
3. **Supervisor failure debounce.** `send_receipt` (the peer-faced fan-out) keeps a
   bounded in-memory map keyed by (account, conversation, receipt face) → last failure
   instant. Within 60 seconds of a recorded failure, another trigger of the same key
   answers `{"status": "unknown"}` immediately — no prepare, no local stamp, no engine
   call. This narrows how often a failing face re-attempts; it retries nothing, so the
   AGENTS.md red line (an unknown outcome must not be retried automatically) is intact —
   the desktop's next deliberate trigger after the window is a fresh request, not a retry.
   The map prunes expired entries on every touch and carries a hard capacity cap, so it
   stays bounded (AGENTS.md).

Boundary notes: the sync read/viewed faces ride the store-side filters (one shared
`prepare_receipt` store path) but not the supervisor debounce — the incident's replay
pressure sits on the peer-faced fan-out, and the self-sync leg keeps its own semantics.
Auto delivery receipts are per-ingest one-shots with no selection and are untouched.
Explicit-`messageIds` receipts are untouched. Error rows: the selection has always skipped
rows whose author cannot be resolved (group sender hashes, §4.28) — unchanged.

Tests: the store arm (the window drops eight-day-old rows from both faces' absent
selection while fresh rows survive, the viewed face skips stamped rows the read face still
selects, the explicit mode still addresses a stamped row, the existing order/bound
disciplines hold on fresh timestamps); the service arm (absent markViewed selects only
fresh unstamped rows and rides exactly that selection into the stamp, absent markRead
selects every fresh row regardless of stamp); the supervisor arm (a marker-driven receipt
failure records the debounce — the immediate same-face replay answers `unknown` with zero
new engine frames, the other face and another conversation are not debounced, a successful
face still confirms `sent`; plus the 60-second expiry boundary at the map level); the
integration arms (the absent read fan-out carries only the fresh row while the stale row
stays silent, the explicit viewed fan-out still carries both rows; the 425-crash degradation
switches to the explicit selection so the crash path stays exercised). Gates: all green.

### 4.120 The identity pending pass-through: `identities.get` rows carry the engine's replace-without-confirmation flag (contract revision 1.136, 2026-10-13)

The engine batch behind the "Identity unverified" send-error family (the §4.61-era
read/write faces' same sidecar — `kt-signal-engine` `identity.rs`) added a per-identity
`pending` key to its sidecar: the receive loop sets it when a peer's identity key is
replaced (the official saveIdentity posture — a verified state degrades to unverified and
the key stays flagged), the trust write face clears it on confirmation (the verified arm
and the `default` arm; the explicit `unverified` arm keeps it), and the send gate refuses
direct sends while the flag stands. The read projection now surfaces that flag, and the
connector forwards it.

The wire change is one optional key on the §4.61 identity row: `pending?: boolean` beside
`trustLevel`. The connector parses the engine's `listIdentities` answer through
`identity_from_engine` with the same pass-through discipline the pinned JsonIdentity
nullable trio (`number`/`uuid`/`scannableSafetyNumber`) already practices — the key rides
verbatim when the engine answered it, stays absent when an older engine answer has none,
and a non-boolean engine value reads as absent (the honest no-signal face; the connector
invents neither `false` nor any grammar, so no legacy desktop ever sees a key it does not
know and no hostile engine value can fail the face). Nothing else moves: no new method,
error code, capability bit, or store move; the read lane, the empty-array = `identity:
null` fold, and the non-nullable core gate (fingerprint/safetyNumber/trustLevel/
addedTimestamp) are byte-identical.

The desktop consumes the key for presentation only: the safety-change banner judgement
(the 2026-10-12 banner batch, `identityUnconfirmedOf`) widens from
`trustLevel === 'UNTRUSTED'` to `trustLevel === 'UNTRUSTED' || pending === true`, closing
the recorded first-contact gap — a peer whose key was replaced before any trust entry
existed projects TRUSTED_UNVERIFIED (the official three-value vocabulary carries no
"changed" signal for a never-verified peer) and used to miss the banner entirely, the
send-failure toast being the only tell. The pending bit is the only honest signal, and it
is presentation judgement on the desktop: the desktop never writes the flag anywhere (the
authority stays the engine sidecar, read back through the next `identities.get` — the
`identity.verifiedChanged` "events notify, the read face is the authority" ruling), and
the confirmed-write local truth flip clears it exactly as the engine's own write face
does (verified/default clear the flag, explicit unverified keeps it).

Tests: the wire-shape unit (`identity_projection_maps_pinned_record_and_empty_list`
family, new `identity_projection_passes_the_pending_sidecar_key_through`): a full record
with `pending: true` round-trips the key on the wire beside the pinned fields; a record
without the key omits it on the wire (older-engine byte-compatibility); a non-boolean
engine value reads as absent; the empty list and the non-nullable core gate are
untouched. Gates: fmt, clippy `-D warnings`, tests (555 lib + 179
integration, all green), release build.

### 4.121 The self-sent sticker and the bodyless pure-quote sent sync: the `sentMessage` mirror stops dropping bodyless sticker and quote sends (no contract revision — wire unchanged, 2026-10-13)

Official behavior: a sticker the phone sends syncs to every linked desktop and renders
there, and a quote-only send syncs the same way. The engine's `SyncMessage.content=Sent`
projection already carried the sticker face (its jsonrpc-compat sent arm), but the
connector's `sentMessage` arm classified a sticker-only mirror as the bodyless catch-all
`skip` and dropped the whole envelope; a pure-quote send (quote, no body, no descriptor)
fell through every arm the same way. Both now land through the existing outgoing ladder —
the same rows the connector's own sends land as, the self send's own timestamp, never an
unread.

The sticker arm reuses the §4.32 incoming projection verbatim (`normalized_sticker` —
one struct, one serde face): the `data` pointer joins the attachment descriptor list as
a regular metadata-only descriptor (byte fetches stay on the existing media channel) and
the pack identity rides the row's `sticker` face — the identical `MessageRecord` shape
the connector's own `messages.sendSticker` pending-row path (§4.31) lands, so the wire
gains zero new keys and the desktop's existing sticker-row consumer is unchanged. The
parse sits deliberately after the control ladder and the state-face arms, so the ladder's
precedence is byte-identical to the text face's: a sticker beside a remote-delete / edit
/ reaction echo keeps its control routing (the control folds onto the row the sticker
landed earlier, never spawning a second one), and a sticker beside an
expiration/group-update/poll rich face keeps the state face. A sticker with no usable
`data` pointer has nothing renderable and keeps the plain skip routing (the incoming
arm's 宁缺勿错 discipline). The bodyless pure-quote arm (no body, no descriptor — the
quote the whole content) lands the same outgoing row a quoted-text send does, quote
snapshot as the row's quote face.

Desktop rendering of the new rows is presentation-only consumption of the existing
`message.upserted` face and is not part of this change. No new method, event, key, error,
capability, or store move; additive receive-plane routing only.

Tests: `sync_sent_sticker_lands_outgoing_row` (sticker-only and quoted-sticker mirrors
land outgoing rows carrying the pack face plus the metadata-only descriptor; the
destination and the sent timestamp key the row), `sync_sent_sticker_keeps_prior_routing_
precedence` (a data-less sticker stays skip; a sticker beside a remote-delete echo stays
control), and `sync_sent_bodyless_quote_lands_outgoing_row` (the quote-only send lands
outgoing with the quote face and empty attachments). Gates: fmt, clippy `-D warnings`,
tests all green.

### 4.122 The passive contacts envelope: `syncMessage.contacts` lands in the contacts cache instead of waiting for the pull (no contract revision — wire unchanged, 2026-10-13)

Official behavior: when a contact or conversation is new, every linked device learns it
immediately — the phone pushes its contact set and the desktop consumes it
(`handleContacts`). The engine already projects that consume as the
`syncMessage.contacts {complete, records[]}` envelope on its receive stream, but the
connector's syncMessage arm ignored the face entirely, so a passively added contact
surfaced only after the next 60s-TTL `listContacts` pull. The face is now intercepted in
`control_routing` like its read/viewed/delete-for-me siblings and consumed into the
existing contacts cache.

The projection bounds mirror the sync-face family: at most `MAX_SYNC_CONTACT_RECORDS`
(512) records per envelope (the tail drops — the pull path still covers it within its
TTL), the ACI each record's hard gate (the engine always projects it; a record without
one is a hostile shape and drops, 宁缺勿错), the e164 and the composed display name
optional and per-key bounded (the receive id family's caps, the pull path's 64-char
title cap). The expireTimer/inboxPosition family is deliberately not read — the
connector's contacts cache holds the identity/title pair only, and the richer trio stays
the throttled `listContacts` enrich's (the §4.8 enrichment lane unchanged). A face with
no usable record at all — including the completion-only envelope, whose `complete` flag
alone is the pull path's concern — keeps the plain syncMessage skip; nothing routes.

Consumption (`apply_sync_contacts`) reuses the pull path's identity ladder and dedup
semantics verbatim: `number` first, the ACI the fallback `peer_key` (so a passive record
converges onto the same cache row a pull wrote), the title the composed display name or
the masked peer id. The same subject twice in one envelope (a number+aci record beside
an aci-only sibling) keeps the named entry — a masked duplicate never downgrades a real
title through the batch's last-write-wins (the same `***` judgment
`title_should_upgrade` applies). The batch commits through
`Store::upsert_passive_contacts`, a sibling of `upsert_synced_contacts` with the same
columns, conflict semantics, and idempotent conversation-skeleton materialization — minus
the sync marker on purpose: `contacts_synced_at` means "a full `listContacts` pull
completed", and advancing it on a passive batch would let the 60s short-circuit report a
freshness the pull never delivered. The store answers exactly the change set — a newly
materialized skeleton or a placeholder title the batch upgraded — and only those
conversations ride the existing `conversation.changed` event, so the desktop's
conversation list converges without a refetch; a replayed batch changes nothing and emits
nothing. No new method, event, key, error, capability, or store move; zero wire change.

Tests: `sync_contacts_routes_bounded_records` (records route with the optional faces,
hostile records drop per-record, the completion-only envelope stays skip, the name caps
at the pull path's title bound), `passive_contacts_batch_keeps_the_pull_marker_and_
reports_changes` (the change set is exactly the new skeletons plus the placeholder-title
upgrades, a replay is empty, the pull marker untouched by every passive write and still
movable by the pull), and `sync_contacts_passive_batch_lands_cache_and_emits_changes`
(the end-to-end consume: new skeletons ride `conversation.changed`, the marker stays
pull-owned, the replay is silent, the masked-title upgrade emits again). Gates: fmt,
clippy `-D warnings`, tests all green.

### 4.123 The automatic send-retry queue: definitively-failed outbound texts drain on the official backoff curve (no contract revision — wire unchanged, 2026-10-13)

Official behavior: Signal's sender retries an undelivered message automatically —
the queue entry waits ~190 ms after the first attempt, re-attempts with a
1.9× exponential curve capped at 15 minutes, and gives up only after 24 hours
(`SignalServiceMessageSender`'s retry session, the sender-held attempt window).
The connector's send funnel historically stopped at the opposite pole: a
deterministic failure answer settled the row `failed` and left it to the host
(the §4.92 manual rearm). The gap the desktop feels is the transient one — a
proxy hiccup, a gateway 502 — where every peer's client quietly recovers and
ours accumulates a manual backlog. The official answer is now the connector's
answer, in the store's own idiom: the queue is the row, not a table — schema 23
adds three columns to `messages` (`first_send_attempted_at`, the window anchor;
`retry_attempt_count`; `next_attempt_at`) plus one partial index
(`messages_retry_queue` on `next_attempt_at, id` WHERE direction='outgoing'
AND status='failed' AND next_attempt_at IS NOT NULL), so a queue entry is a
failed row carrying its schedule and an entry's death is a column clear, never
a delete.

Eligibility is the deterministic-error-answer family and only it:
`EngineError::ServerUnavailable` and `EngineError::Upstream` — the engine
answered that the API call failed before any dispatch reached the wire, so the
outcome is definitively "not sent" and a resend cannot double-deliver. The
deviation ledger, deliberately narrower than the official curve's error set:
`UpstreamCode`, `RateLimited`, `ProofRequired`, and `IdentityUnverified`
stay §4.92 manual (each carries a product judgment the funnel must not make
silently — a rate-limit pause the curve would turn into a hammer, a proof
request the host must surface, an identity gap the safety banner owns); a
mutating send that times out classifies `UnknownOutcome` (the §4.80 anchor
shape) and is never enrolled — the red line (an unknown send outcome must not
be retried automatically) outranks the queue at every layer; a partial-recipient
group failure resupplements nobody (the row settles failed whole; a per-recipient
retry ledger is a future revision's problem). (Contract 1.143 later migrates
the rate-limit family out of this ledger onto its own Retry-After beat —
§4.139 — precisely because the curve beat was the hammer the ledger feared:
the pause the server asked for is the schedule, not a curve head.)

The curve is the official one in exact integer arithmetic:
`min(190 ms · 1.9^(n-1), 15 min)` (`Store::retry_backoff_delay_ms` — the cap
binds from attempt 15, which is what fits ≈110 attempts inside the window).
The window is anchored on `first_send_attempted_at`, stamped only by the first
automatic enrollment and never moved by a retry, and judged at claim time:
a row whose anchor is past 24 h has its `next_attempt_at` cleared and stays
`failed` (window exhaustion is death, not an error). The ceilings are bounded
twice over: `MAX_RETRY_ATTEMPTS` (128 — a safety net an order of magnitude
above the ≈110 the curve fits in 24 h, for a clock that jumps or a curve that
someday tightens) per row, and `MAX_RETRY_QUEUED_PER_ACCOUNT` (256) live
entries per account, enforced by the schedule arm — an over-ceiling enrollment
is refused by clearing that row's `next_attempt_at` only (the window anchor and
attempt count survive as the terminal state's record; the row stays failed and
manual). The enrollment is atomic with the settlement:
`Store::settle_send_failed` performs the guarded `pending → failed` flip and
the schedule write in one transaction, so no observer — the claim pass
included — can ever see a row failed-but-unscheduled between the funnel's
settle and the queue's arm; the ineligible arm is the same write with the
schedule cleared.

The drain is one bounded worker per process (`spawn_outbox_worker`, spawned
beside the delivery-receipt worker): a 15 s tick plus a wake-at-earliest-due
read after every pass (`next_retry_due_at`, floored at 50 ms so a just-scheduled
190 ms beat is honored at its own pace, never the tick's), claiming at most
`OUTBOX_CLAIM_BATCH` (16) due rows per pass through `Store::claim_due_retries`
— an atomic multi-row take that hands each row to exactly one pass (the
store-level double-take test proves the second claimer gets nothing). A claim
rebuilds the send from the persisted row through the same
`rebuild_retry_dispatch` face the manual rearm uses — one code path for both
retries, the same §4.92 state guards — and settles through the ordinary send
funnel: success runs the standard `complete_outgoing_send` (which clears the
schedule face), a still-retryable error re-arms through
`settle_send_failed` with the count bumped, an unknown retires the schedule
(`retire_retry_schedule` — the red line's second lock, so a row that drifts
into the indeterminate family mid-queue loses its live schedule in the same
breath), and the attempt ceiling's refusal arm clears the schedule and leaves
the row failed-final. An offline engine never judges the queue: the pass
aborts before claiming when no engine is up, so entries keep status and
schedule across downtime. Three hooks make recovery prompt rather than
tick-paced: the engine landing on `start()`, a successful `restart_engine`,
and the funnel's own retryable-failure arm each kick the drain (a cap-1 lossy
channel — a burst of kicks collapses to one immediate pass, bounded by
construction).

Restart semantics close the loop with §4.92: already-queued (failed +
scheduled) rows survive restart and resume their curve on the first engine
land, while pending rows swept at boot stay manual-only — that boundary is
§4.124's subject below.

Tests: `retry_window_exhaustion_kills_the_queue_entry` (the 24 h judgment
clears the schedule and keeps the row failed; a fresh anchor survives retries),
`retry_claim_is_atomic_against_double_take` (one due row, two claimers, one
winner), `retry_queue_survives_restart_while_swept_pendings_stay_manual`
(queued rows resume, pending rows do not enroll), `retry_queue_bounds_are_
enforced` (the per-account ceiling refuses by clearing the schedule only, the
attempt ceiling kills at 128, both leave the row failed), and
`schema_v23_upgrade_adds_retry_queue_columns` (the versioned migration stamps
the columns and index on a v22 store); `retry_queue_enrolls_only_the_eligible_
error_classes` (ServerUnavailable/Upstream arm the schedule, every other
class settles plain-failed), `unknown_outcome_retires_any_stale_retry_
schedule` (the red line's store-side second lock), `outbox_claim_rebuilds_
the_manual_retry_dispatch` (one dispatch path for both retry families); and
the `outbox_worker` integration trio `engine_land_drains_a_queued_failed_row`
(the hook drains a due entry the moment the engine lands, one history row),
`definitive_failure_requeues_and_the_auto_retry_succeeds` (the end-to-end
loop: unavailable answer → enrolled → the 190 ms beat retries → sent, two
upstream sends, one row), and `offline_engine_leaves_the_retry_queue_untouched`
(no engine, no claim, schedule byte-identical across the wait). Gates: fmt,
clippy `-D warnings`, tests all green.

### 4.124 The restart line stays manual: swept pending rows are not re-enrolled into the §4.123 queue (no contract revision — wire unchanged, 2026-10-13)

§4.92's rule stands: a connector restart sweeps its in-flight `pending` rows
to `failed`, and the host re-arms them through `messages.retry` when a human
decides to. The automatic queue deliberately does not absorb them, and the
reason is the red line's edge case. A `pending` row at the moment of process
death has an unresolved wire outcome: the send was dispatched to the engine,
the engine may have handed it to the server, and the connector died before any
answer — the §4.80 unknown-outcome family by construction. The official
sender survives the same crash safely for one reason the connector cannot
borrow: it re-sends with the original client-side timestamp, and the server
deduplicates on that timestamp, so a message that actually landed is absorbed
as a re-delivery of the same send. The pinned `signal-cli` send surface offers
no such lever — `SendCommand.java` (verified against
`.references/signal-cli/src/main/java/org/asamk/signal/commands/SendCommand.java`)
accepts quote, story, and edit timestamps but no message timestamp, so a
blind re-dispatch of an unknown-outcome row is not a retry; it is a fresh
send with a fresh timestamp and a real chance of double delivery. The queue's
eligibility family (§4.123) never contains such a row precisely because the
funnel enrolls only deterministic error answers — the restart sweep is the
one path that manufactures unknowns in bulk, and it stays outside the queue
for the same reason unknowns do.

The evolution path is the engine's, not the connector's: if a future
signal-cli/engine revision exposes a client-timestamp (or idempotency-key)
send face, the sweep can hand its rows to the queue with their original
timestamps and the official crash-recovery semantics become expressible — a
future revision arms it, the way §4.123 armed the deterministic family. Until
then the manual rearm is not a gap but the honest face: the one human who
knows whether the message landed decides, and `messages.retry` re-enters the
ordinary send path with all its guards.

Tests: `retry_queue_survives_restart_while_swept_pendings_stay_manual` (the
store-level boundary: queued rows resume, swept pendings stay failed and
unscheduled), shared with §4.123; no new tests — this section records a
boundary deliberately kept, not a face added. Gates: fmt, clippy
`-D warnings`, tests all green.

### 4.125 The batch-C emit surface: the desktop-facing gaps close in order (contract revision 1.137, 2026-10-13)

The desktop parity matrix's batch C is the connector's emit face — every
surface below rides the connector→host wire (the `MessageRecord` /
`ConversationSummary` shapes and the host events), never the
connector→engine leg. This revision lands the batch's two send faces and
its one verification record: the outbound link-preview `date` stamp
(§4.126), the outbound attachment blur placeholder (§4.127), and the
admin-delete-window verification (§4.133 — a verification record, no
implementation). The batch's remaining faces land in order: the typing
summary projection (§4.128), the conversation-summary identifiers
(§4.129), the draft timestamps (§4.130), and the blocked mask (§4.131)
landed together at revision 1.138; the live contact-card branch (§4.132)
completes the §4.125–§4.133 run at revision 1.139 — the numbering gap
between 1.137's sections and §4.133 was that landing order, not a
reserved hole.

### 4.126 The outbound link-preview date rides the send (contract revision 1.137, 2026-10-13)

The receive half of the preview card has ridden message rows since 1.107
(§4.88: `NormalizedPreview.date`, positive-only, the engine's P14
projection). The send half was date-less: the desktop's composer stamps
the preview card it renders in its own bubble, and the stamp vanished at
the wire — the optimistic row and the eventual mirror disagreed about
the card's publish date.

- Params: `messages.sendText` preview entries gain the optional `date`
  (u64 epoch-ms). `0` reads as absent — the official
  `cleanLinkPreviewDate` falsy posture (a zero stamp carries no
  information); a negative, fractional, or non-integer value fails
  params deserialization (the u64 type, fail-closed).
- Upstream: the engine `send` `previewDate` key (verified against the
  engine's own send-face parse: a u64 `> 0` is kept, `0` folds to absent
  — the same falsy posture — a non-integer answers the internal-error
  face, and the key must ride `previewUrl`, the quoteType host-object
  precedent). The connector forwards a positive value verbatim and omits
  the key when absent, so every pre-1.137 send shape stays
  byte-identical.
- Pending row: the optimistic row's rich now persists the preview card —
  `previews[0]` carrying `url`/`title`/`description`/`date` with
  `image` always absent (the image data URI is transit-only material;
  the §5.10 budget bounds the hop, not a store). Previously the pending
  rich armed only for view-once and text-style sends; a preview-bearing
  send arms it too. Pre-1.137 rows read byte-identically (the key set is
  unchanged, the card simply exists now).
- Retry: the shared retry-dispatch rebuild — the §4.26 manual face and
  the §4.123 queue drain both ride it — restores the preview from the
  row rich (`url`/`title`/`description`/`date`, never the image bytes:
  upstream `previewImage` is independently optional, so the retried card
  re-sends metadata-only). The honest deviation, documented: the image
  is lost on retry, because persisting a 1.5M-char data URI in the row
  would break the no-bytes-in-rows rule the store is built on. Rows
  persisted before this revision carry no rich preview, so their retries
  stay preview-less — the pre-1.137 behavior, unchanged.
- Schema: `messagesSendTextParams` declares the `previews` array — a
  backfill: the code has accepted the key since the §5.10 v1.26 preview
  face, but the schema def never declared it. This revision declares the
  full entry shape (required `url`/`title` at the §5.10 bounds, optional
  `description`/`imageDataUri`, optional `date`; only the first entry is
  used, extras ignored — the signal-cli one-preview discipline) so the
  wire contract and the code agree in both directions. The four preview
  bounds join the schema numeric-bounds pairs; `date`'s floor is 0 by
  type, its falsy read is behavior (pinned by tests, not by a schema
  number).

### 4.127 The outbound attachment blur placeholder (contract revision 1.137, 2026-10-13)

The receive half of the blur placeholder has ridden attachment
descriptors since 1.107 (§4.88). The send half had none: the desktop
renders outgoing media from the first pending tick and holds a blur
placeholder until the mirror lands — the descriptor had no key to carry
one, so the sender's own bubble was the one surface in the product that
could not blur.

- Params: `messages.attachments.send` gains `blurHash` on the singular
  payload and `blurHash` on each plural `attachments[]` entry —
  optional, 1..128 characters (the engine's own
  `MAX_ATTACHMENT_BLUR_HASH_CHARS`, the receive face's bound verbatim:
  one constant, both directions). An empty string is INVALID_REQUEST
  (omit the key instead), an over-bound value is INVALID_REQUEST; the
  connector never decodes or charset-validates a blur string — the
  receive posture verbatim: the desktop computes it, the connector ships
  it.
- Form exclusivity: a top-level `blurHash` together with the plural
  `attachments` array is INVALID_REQUEST (the form-match tuple widens;
  per-entry keys ride the entries, exactly like `filename`/`contentType`
  before it).
- Pending row: the descriptor carries `blurHash` from the first pending
  tick (both descriptor sites — singular and plural — fill it), so the
  sender's bubble renders the placeholder before settlement and across
  restarts; the settled row keeps it until the mirror (which carries the
  engine's own receive-face blur keys) converges the row.
- Upstream gap, documented and deliberately not invented: the engine's
  data-URI attachment channel carries no blurHash key (the verified
  data-URI parameter syntax is `filename` and the `;voice=true`
  parameter, nothing else). Upstream recipients compute their own
  placeholders when the media lands. The connector mints no upstream
  key; this face is the sender's own bubble placeholder, and the gap is
  the upstream channel's, not the connector's.
- Schema: both defs gain `blurHash` (1..128); the numeric-bounds pair
  pins the bound to the shared engine constant.

### 4.128 The typing projection carries the sender (contract revision 1.138, 2026-10-13)

The `conversation.typing` event (§4.13) told the desktop *that* a
conversation was typing, never *who*. The desktop list row renders the
indicator with an author label, and two peers typing into one group are
two facts, not one — so the event now carries the sender the envelope
named.

- Event: `conversation.typing` additively carries `senderId` (the
  envelope source, verbatim — ServiceId UUID or e164 as received) and
  `senderName` (the envelope `sourceName`, the same trim/64-char bound
  the engine projection applies to every actor-name capture). Both keys
  are absent when the envelope carried neither — absent keys keep the
  host merge total, and pre-1.138 hosts read byte-identical payloads.
- Source: the fields ride the `NormalizedReceive` the control
  projection already builds (the same capture every message row's
  actor name uses) — no engine change, no routing change, the typing
  arm reads what the envelope said.
- The rate limiter stays conversation-scoped (unchanged, the 1.15
  discipline): inside one window the first sender wins, and a second
  sender's indicator converges on its next upstream refresh — typing
  indicators re-fire every few seconds, so the collapse self-heals
  faster than a human notices. The host's display timeout remains the
  presentation owner.
- Schema: the `conversation.typing` data def gains the two optional
  keys (the required trio unchanged).

Tests: `typing_event_carries_the_envelope_sender_identity` (the named
envelope carries both fields, the bare envelope keeps the name absent),
beside the 1.15 limiter tests.

### 4.129 The conversation summary carries its identifiers (contract revision 1.138, 2026-10-13)

The desktop's `!` selector faces match on identifier suffixes —
`*ServiceIdEndsWith`, `*E164EndsWith`, `*GroupIdEndsWith` — and the
summary row carried none of them: the desktop derived what it could
from its own cache and the commands sat unimplemented (the parity
matrix's data-plane gap). The row now carries its identifiers, derived
at read time from the peer key's shape — the peer key is already the
row's address, so the derivation is one source, no second truth:

- `groupId`: a group row keys it off the raw group master key — the
  §4.44/§4.50 addressing form, verbatim.
- `e164`: a direct row keyed on a `+`-prefixed phone number answers it
  verbatim.
- `serviceId`: a direct row keyed on a ServiceId UUID answers it
  verbatim; a direct row keyed on a number-shaped key (history-
  imported, no live envelope seen yet) maps through the most recent
  `peer_identities` row the ingest wrote — the §4.48 reverse lookup. A
  key nothing resolves leaves the row with no identifier: the row stays
  addressable by id, and no identifier is invented (the lookup is a
  tolerant read — a failure degrades to absent, it never fails the
  summary).
- `pni`: reserved and never present. The connector has no PNI source —
  this is the honest hole the section documents so nobody later "fills"
  it with an invented key.
- Serialization: the four keys are absent-key (`skip` when None), so
  pre-1.138 hosts read byte-identical rows and a row without a
  resolvable identifier carries nothing.
- Both summary builders share the derivation (the list page and the
  single-row `conversation.changed` summary cannot disagree — the 1.71
  read-time family discipline).

Tests: `conversation_summary_projects_identifiers_from_the_peer_key`
(the five shapes: group / `+` / UUID / number-mapped / orphan).

### 4.130 The draft stamp rides the summaries; saveDraft/clearDraft are local faces (contract revision 1.138, 2026-10-13)

The official DraftRepository records a draft as (conversation, text,
timestamp). The text is the desktop's own safeStorage material — the
connector is body-blind to it by construction (the §6.5 no-PII
discipline applies double to unsent text) — but the *timestamp* is the
list's sort/label signal and it had no wire. Two new local faces write
it, and the summaries carry it:

- `conversations.saveDraft` `{accountId, conversationId,
  draftTimestamp}`: the stamp is required and must be positive (0 is
  the absent fold — clearing is `clearDraft`'s explicit job, a 0 stamp
  answers INVALID_REQUEST). Result `{status: "saved"}`.
- `conversations.clearDraft` `{accountId, conversationId}`: idempotent
  — a conversation with no stored draft still answers
  `{status: "cleared"}`, a replay stays silent.
- Lane: local state with no engine and no upstream — the
  `history.importRetry` shape (send lane, account-scoped mutating: the
  same-account write mutex and delete barrier apply through the host
  `acquire`; the store work runs on the blocking pool). The
  conversation row resolves first (CONVERSATION_NOT_FOUND before any
  write); re-saving the same conversation replaces its stamp.
- Storage: `conversation_drafts(account_id, conversation_id,
  draft_timestamp, PK(account_id, conversation_id))` — the
  `conversation_pins` table shape minus the pin identity columns,
  `CREATE TABLE IF NOT EXISTS` in the open DDL (no user_version move;
  the `peer_identities` precedent). One row per conversation,
  replace-on-save; the account/conversation foreign keys cascade.
- Summaries: `draftTimestamp` (epoch ms, absent when nothing is drafted
  or after a clear), and both writes push the refreshed summary on
  `conversation.changed` so every window sees the new draft clock
  without a re-read (the expire-timer mirror discipline).

Tests: `conversation_draft_stamps_roundtrip_through_summaries` (the
store boundary), `store_draft_faces_bound_resolve_and_stamp` (bounds
and refusals), `save_and_clear_draft_stamp_locally_and_push_changed`
(the supervisor lane and the event).

### 4.131 The blocked mask reaches the list and the event stream (contract revision 1.138, 2026-10-13)

The blocked bit lived in three places — the §4.45 pull (cloud records),
the §4.47 write (upstream), and the enrich's own `isBlocked` trio stamp
— and none of them told the desktop's list until it pulled. The summary
now carries the mask state, and the writers that move it push the
refreshed summary:

- Summary: `blocked`, a bare always-serialized boolean (the
  muted/pinned/isSelf discipline — false is the meaningful unmasked
  default): the direct row's stored `mrr_factors_json.blocked` stamp.
  Group rows stay false — §4.45 pull remains the group authority (the
  honest hole; a group mask still read-backs on the §4.47 response
  body, and the desktop's own cache holds the pull result).
- Writer 1, the §4.47 write-back: a definite `conversations.setBlocked`
  success now mirrors the bit into the direct row's factor stamp and
  rides `conversation.changed` (the expire-timer local-mirror
  discipline: a mirror failure only costs the next event its freshness,
  the cloud answer stays the face). A group conversation mirrors
  nothing.
- Writer 2, the §4.58 consume batch: the BLOCK family verbs —
  `BLOCK=3`, `BLOCK_AND_DELETE=4`, `BLOCK_AND_SPAM=6` (the official
  `SignalService.proto` member values) — stamp `blocked=true` through
  the same consume batch as the accept stamp, so a linked device's
  block composition masks the conversation locally too, whichever
  device composed it. The face stamps true only: the sync vocabulary
  has no unmask member, and unblocking is writer 1's and the enrich's
  job.
- Preservation: the throttled enrich's trio re-stamp now preserves the
  `blocked` key exactly like the accept pair — each key family has one
  writer, so an enrich that stamps the probed-absent `{}` can never
  clear a mask a write face just set, while an engine answer that does
  name `isBlocked` still wins (the cloud truth at enrich time).
- Additive posture: pre-1.138 desktops that never read the key lose
  nothing; pre-1.138 connectors never sent it.

Tests: `blocked_stamp_survives_the_enrich_and_yields_to_engine_truth`
(the store merge/preserve semantics),
`ingest_message_request_response_block_family_stamps_blocked` (the
consume batch, and a non-BLOCK verb never rewrites the mask),
`set_blocked_write_back_mirrors_the_local_stamp_and_pushes_changed`
(the write-back mirror and the event, both directions).

### 4.132 The live contact-card branch: the shared contact reaches the live rows (contract revision 1.139, 2026-10-13)

The §4.38 import chain has landed contact cards since 1.78 — the
archive's per-message `contact` face parsing onto the bounded
`NormalizedContact` shape — but the live receive chain never parsed the
face: a peer sharing a contact over the wire produced a plain (usually
skipped) row and the desktop never saw the card. This revision closes
the gap by reusing the import authority itself, not by restating it.

- One authority: the live branch calls the import chain's
  `import_contact` reader directly (now `pub(crate)`), so the bounds are
  the contract's, stated once — the `MAX_CONTACT_*` caps, the
  eight-entry ceiling, the HOME/MOBILE/WORK/CUSTOM type vocabulary with
  unknown coercing to HOME (the official `phoneToContactFormType` parse
  face), the whole-card discipline (a card with no usable content
  answers no face and the envelope keeps its plain routing — never a
  placeholder card), and the per-entry drop. The live face adds one
  wrapper: the engine projects the avatar as
  `{avatar: <pointer>, isProfile}`; the pointer unwraps to the plain
  archive shape and rides `normalized_attachment` like any descriptor
  (byte fetches stay on the existing media channel), and `isProfile` —
  the engine's address-book fact, not the message's — never rides the
  row. A wrapped avatar whose pointer is absent unwraps to no avatar
  face.
- Incoming: the `dataMessage.contact` face parses beside the sticker's
  (§4.32's list discipline — the pointer joins the attachment descriptor
  list exactly like a sticker's `data`). A bodyless card lands as a real
  incoming message row through a new arm checked after the state faces
  (the timer/group-update/poll markers keep their precedence — a state
  marker never coexists with a card); a body-carrying message keeps its
  row with the card riding `rich.contact` beside the text.
- Sent mirror: the `syncMessage.sentMessage.contact` face parses the
  same way, after the control ladder and the state-face arms, so a card
  beside a remoteDelete/edit/reaction echo keeps its control routing
  (the merge happens after that ladder read — the sticker
  descriptor-push discipline) and a bodyless contact-only send lands as
  the same outgoing row a peer's card does, the self send's own
  timestamp, never an unread (the 自发同步 posture).
- Budget: the queue estimator (§4.13) counts the card's heap strings
  entry-wise — the struct, the six name parts, each phone/email entry's
  value + label + field struct, the organization — the previews/mentions
  pattern, so one oversized contact card cannot dodge the queue budget.
- Store and wire: `NormalizedRich.contact` has ridden rows since 1.78
  (the additive flatten channel), so no store schema move and no new
  wire key — live rows simply begin carrying the `contact` key the
  import rows already carried.

Tests: `live_contact_card_lands_a_message_row_in_the_history_shape` (the
full card, the wrapped-avatar unwrap, `isProfile` never on the wire, the
budget delta, text+card, the pointerless avatar, the degenerate faces),
`sync_sent_contact_card_lands_outgoing_row_and_keeps_control_precedence`
(the outgoing row and the control-echo precedence).

### 4.133 The admin-delete window is a client-side remote-config gate, not a protocol face (verification record, 2026-10-13; no contract change)

The batch's item eight asked whether the official delete-for-everyone
time window is something the connector must enforce. Verified against
the official desktop and the protocol: it is not, and this section is
the record so nobody "fixes" the connector's no-precheck posture later.

- Official desktop: the window lives in `ts/util/getDeleteMaxAgeMs.dom.ts`
  — `getAdminDeleteMaxAgeMs()` reads the `global.adminDeleteMaxAgeInSeconds`
  remote config (positive seconds; anything else falls back to one DAY)
  — and `ts/canDeleteForEveryone.dom.ts`'s `canUseAdminDelete` enforces
  `messageAge <= getAdminDeleteMaxAgeMs() + gracePeriodMs` beside the
  GV2-group and admin-role checks. Purely presentation-layer: the
  official client stops offering the delete past the window.
- Protocol wire: `DataMessage.adminDelete
  {targetAuthorAciBinary, targetSentTimestamp}` (§4.28's field 29)
  carries no age field, and the server enforces none — an older delete
  is accepted exactly like a fresh one.
- Engine: `send_admin_delete_group` (engine `src/service/mod.rs:1631`)
  composes the proto directly, no window check.
- Connector: §4.28's posture — "target eligibility against admin state
  is not pre-checked — the server is the authority" — already matches
  the wire truth. The window is the composing client's own gate.
- Decision: no implementation. The window belongs to the desktop's
  composition UI, which owns the remote config and the button; a
  connector-side window would be a second, divergent authority reading
  none of the config the real gate reads.

### 4.134 The attachment-caption retry guard: a failed attachment row never enters the §4.123 queue (contract revision 1.140, 2026-10-13)

The §4.123 audit found a correctness hole at the queue's edge: the
enrollment gate (`Store::settle_send_failed_schedule`) judged eligibility
on `body` non-empty alone, so a failed `messages.attachments.send` row
carrying a non-empty caption enrolled exactly like a text row — and the
rebuild face (`rebuild_retry_dispatch`, shared by the manual §4.26 rearm
and the automatic §4.123 drain) reconstructs text, quote, and the §4.126
preview card only. The attachment bytes (and their engine data-URI
channel) have no home in the rebuilt dispatch, so the drain would have
re-dispatched the caption as a plain text send: the recipient gets the
caption's words without the picture, and the original row settles `sent`
over a message that never carried its attachment. The guard closes the
hole at every layer, one rule — an attachment row is never retried:

- Enrollment (store): the schedule arm reads the row's persisted
  `attachments_json` beside `body` and refuses the descriptor-carrying
  family — the refusal is the standard retire shape (the schedule clears,
  the row stays `failed` and manual, the §4.92 rearm is still the honest
  answer), so a refused row is terminal for the automatic face, never a
  pause. A caption-less attachment row was already ineligible (empty
  body); the caption no longer smuggles it in.
- Claim (store): `claim_due_retries` re-checks the same face before the
  atomic flip, because rows enrolled before this revision can still carry
  a live schedule. A refused row retires its schedule and stays `failed`
  (the over-ceiling refusal shape) — the queue hands it to no rebuild,
  and the scan shrinks instead of circling.
- Rebuild (service): `prepare_outbox_retry_text` re-checks after the
  claim's `failed -> pending` flip (belt over the claim's braces — a row
  that reached a claim through a shape the store could not read). The
  refusal settles the deterministic failure in the same breath
  (`settle_send_failed` no-schedule arm: the guarded flip back to
  `failed` clears the entire queue face), so a legacy row can never
  strand as pending; the worker logs the drop and moves on.
- Manual face (service): `messages.retryText` refuses the same family
  with a deterministic `RETRY_NOT_ALLOWED` — the manual rebuild shares
  `rebuild_retry_dispatch` and would resend the identical caption-only
  shape, so the human re-sends the attachment instead (the §4.92 answer).

No store schema move (`attachments_json` has ridden rows since 1.15), no
wire change: the queue is connector-internal and the refused rows keep
the exact manual-line shape §4.92 already renders. Zero wire change.

Tests: `retry_queue_never_enrolls_an_attachment_caption_row` (the
captioned attachment send fails retryably and settles plain-failed with
the schedule retired — a sibling text row enrolls beside it and the claim
takes only that one), and
`retry_faces_refuse_an_attachment_row_and_settle_the_deterministic_failure`
(a pre-revision enrolled row: the claim-time gate retires it before any
flip; a row that slips through to a rebuild is settled failed with its
queue face cleared and the manual face answers `RETRY_NOT_ALLOWED`).
Gates: fmt, clippy `-D warnings`, tests all green.

### 4.135 The pull path rides `conversation.changed`: skeleton materialization is no longer silent (contract revision 1.140, 2026-10-13)

The §4.122 passive batch taught the store to answer exactly the skeleton
change set — newly materialized skeletons and placeholder-title upgrades —
and pushed each one as `conversation.changed`, so the desktop's list
converged without a refetch. The pull path (`contacts.sync`, the startup
re-sync, and the shared `sync_contacts_with` body) wrote through the
sibling `upsert_synced_contacts` and threw that answer away: after an
engine restart or a fresh link, the pull materialized every skeleton and
the desktop — already rendering its list — learned the new rows only on
its own next fetch. The pull now mirrors the passive batch's semantics
verbatim; one change-set discipline, two writers:

- The store's `upsert_synced_contacts` returns the same change set
  `upsert_passive_contacts` returns: a conversation id per newly
  materialized skeleton and per placeholder title the batch upgraded
  (the `title_should_upgrade` judgment — a real title never downgrades).
  The sync marker still advances in the same commit — only the return
  type changed — so the 60 s short-circuit and the all-or-nothing
  transaction are untouched.
- `sync_contacts_with` turns the change set into one `conversation.changed`
  per row (summaries read after the commit, the §4.122 shape) and rides
  the existing host event lane — the same lane the passive batch and the
  timer faces use; no new event, no new key. The host-facing
  `contacts.sync` response is byte-identical (`ContactsSyncOutcome`):
  the events are a side lane, not a response shape change.

Replay silence is structural: a re-pulled batch finds every skeleton
present and every title real, the change set comes back empty, and
nothing emits — the short-circuit makes the common replay a no-op before
the store is even reached. Zero wire change.

Tests: `synced_contacts_pull_reports_the_changed_skeleton_set` (the
store-level change set: both new skeletons, the empty replay, the
masked-title upgrade, the marker still pull-owned), and
`pull_contacts_sync_emits_conversation_changed_for_new_skeletons`
(the supervisor-level consume against the fixture engine: the pull's
skeletons ride `conversation.changed` on the host lane, a second pull
inside the short-circuit window emits nothing). Gates: fmt, clippy
`-D warnings`, tests all green.

### 4.136 The `syncMessage.blocked` consume arm: the linked device's block list masks locally too (contract revision 1.140, 2026-10-13)

The engine already projects the official `_handleBlocked` mirror
(MessageReceiver.preload.ts:3988) on its receive stream — modern and
legacy family shapes, the official `areModernFieldsUsed` selection — but
the connector's syncMessage arm dropped the face entirely, so a block
composed on the phone never masked the conversation on this device until
some other writer (the §4.47 write face, the enrich's own `isBlocked`)
happened to refresh the bit. The arm now consumes like its
read/viewed/contacts siblings: intercept before the plain syncMessage
skip, mirror into the §4.131 blocked factor store, ride
`conversation.changed`.

- Parse (tolerant): both families union — modern
  `blockedE164s[{e164, timestamp?}]` / `blockedAcis[{aci, timestamp?}]` /
  `blockedGroups[{groupId, timestamp?}]` entry objects, legacy
  `numbers` / `acis` / `groupIds` / `acisBinary` string arrays (the
  engine already resolved the binary forms to UUID strings). Unknown keys
  and unknown entry fields are not fatal (the official parse is loose the
  same way); an entry whose identity read fails drops per-entry; each
  class dedups and caps at `MAX_SYNC_BLOCKED_ENTRIES` (512, the read-face
  truncate discipline — a real block list is user-scaled tens, so the cap
  only ever bites on a hostile face). The modern entries' advisory
  `timestamp` has no home in the connector's factor stamp and does not
  ride. A face whose every key failed keeps the plain syncMessage skip —
  the fully-empty list (the official projects the modern empty object for
  it) is exactly that shape.
- Consume (presence stamps true only): each e164/ACI identity resolves
  through the sync ladder family (the identity itself, the recorded
  ACI→peer-key mappings, then contacts digit-suffix matches — the
  §4.37/§4.81 shape) and every resolved direct row stamps the blocked
  factor `true`, so a two-keyed subject converges both row forms. This is
  the §4.58 BLOCK-family verbs' 盖真不盖假 discipline, deliberately: the
  official treats the sync as a full-list replacement, but the
  connector's factor store has no list to replace and a partial sync
  inferred as "absent means unblocked" could unmask a conversation the
  §4.47 write face masked — unblocking stays the §4.47 write's and the
  enrich's own `isBlocked` to clear, exactly as §4.131 assigns it. A
  conversation whose bit already reads true stamps nothing and emits
  nothing (replay silence by construction); a stamped one rides
  `conversation.changed` with the refreshed summary.
- Group half (deliberate non-consumption): the factor stamp is
  direct-rows-only and the group summary's blocked bit stays
  constant-false by design — a group block keeps riding the §4.45 pull as
  its authority, the honest hole §4.131 documents. The group identities
  consume silently; no new event, no new key.

No new method, event, key, error, capability, or store move; zero wire
change.

Tests: `sync_blocked_routes_both_families_and_tolerates_unknown_fields`
(the routing arm: modern objects, legacy arrays, the union, the dedup,
the unknown-field tolerance, the identity-cap truncation, the
fail-closed skip for an empty or hostile face), and
`ingest_sync_blocked_stamps_the_factor_and_rides_conversation_changed`
(the consume: the factor stamps and the summary rides
`conversation.changed`, a replay is silent, an unknown identity drops
warn-only, the group half consumes nothing, no message row is ever
created). Gates: fmt, clippy `-D warnings`, tests all green.

### 4.137 The storage pull outcome reaches the host: `engine.storagePull` rides the account-domain transient lane (contract revision 1.141, 2026-10-13)

The engine's T-9b batch (the storage-manifest pull leg, engine
`b719997`, joined by the result-event emission this batch pairs with)
fires a standalone notification once per manifest pull attempt — the
login-time automatic pull (`trigger: "login"`) and every
`fetchLatest`-triggered pull — and the connector used to answer the
unknown method with the plain protocol warning. The event now rides the
§4.117 account-domain transient lane end to end, the same posture the
emptied/clockskew/conversationState trio already runs: the parse arm
emits an attributed `EngineEvent::StoragePull`, the supervisor's
engine-event watcher hands it to the service, the service resolves the
signal account through the store (the shared
`account_transient_forwarded`/`_unattributed` counters — an unbound
account drops warn-only, never claimed as forwarded), and the host frame
`engine.storagePull` `{accountId, trigger, contactsOk, groupsOk}` rides
the shared event stream.

The parse guarantees exactly the faces the engine contract promises and
deliberately nothing more: the attribution account (non-empty string —
the malformed family falls back to the unknownNotification warning), the
`trigger` word, and the two per-family result legs (`contactsOk` /
`groupsOk`, booleans). The trigger word is typed but its value domain is
deliberately unlocked: the engine's current vocabulary is `login` /
`fetchLatest`, and the connector neither pins nor translates it — an
engine trigger word this connector has not seen forwards verbatim,
because a guessed rewrite would tell the desktop a lie about which pull
finished, and a schema-enforced pair would reject the very frames the
honest-forward posture exists to deliver (the schema declares the pair
descriptively, not as an enum). A missing or wrong-typed face (no
account, a numeric trigger, a string leg, a missing `groupsOk`) is a
protocol error answered by the unknownNotification warning, never a
guessed forward — the §4.117 discipline verbatim.

The event is transient on the connector side like every lane member: no
row, nothing persisted, nothing self-heals; a missed event converges
through the ordinary pull faces the next time the desktop reads them.
The desktop's consumption is its own: with the truthful per-family legs
it can skip its post-pull refresh work when the pull already landed
fresh, and it can schedule one when a leg failed, without polling.

Wire additive-only: one new host event name (`engine.storagePull`), no
new method, error, capability, or store move; the perf word list grows
nothing (the lane's two counters are the honest face). Zero engine-side
coupling beyond the pinned notification shape (`params` = account /
trigger / contactsOk / groupsOk).

Tests (the four transient-lane tests grow the storagePull assertions in
place, the 1.119 extended-tests posture): the engine parse arm (the
login pull and an unseen trigger word forwarded verbatim; the malformed
family — missing/empty account, numeric trigger, string `contactsOk`,
missing `groupsOk` — into the unknownNotification warning), the service
forward arm (the bound account resolves, the unbound account drops with
both perf counters), the host wire frame (the verbatim trigger beside
the two legs), and the end-to-end harness over the real engine process
(the fixture's raw-notification marker grows the storagePull entry; the
frame rides with the resolved accountId, and the malformed face joins
the unattributed non-forward batch as a protocol warning). Gates: fmt,
clippy `-D warnings`, tests all green.

### 4.138 The account's own ACI: carried when the engine answers it, absent when it does not (contract revision 1.142, 2026-10-13)

The desktop has an account-level identity hole the connector could not
fill: the own ACI — the UUID the account's peers address it by (the call
self-UUID, group member rosters, mention ranges) — never appeared on any
connector face. The store's account rows answered the gap with the
opaque local row id, and the §4.24 boundary is explicit about why: the
own ACI is not queryable on either engine's jsonRpc surface, so that id
is a random 32-hex label, NOT an ACI, and was never derivable from
anything the connector holds.

This revision keeps that honesty and adds the carry-through a future
engine answer needs without another contract move — contract-first,
because the pinned engine (`b719997`) answers no ACI anywhere: finishLink
answers `{number}`, listAccounts `[{number}]`, and the truth (the presage
store's `registration_data` `service_ids` aci) has no jsonRpc read face.
The connector therefore learns exactly one new thing: an optional `aci`
key beside the number on both faces, and the rules for carrying it.

- The parse-and-normalize ladder: a string value that parses as an ACI —
  the dashed 8-4-4-4-12 shape or the bare 32-hex shape, either case —
  normalizes to the dashed lowercase form (the shape the engine's
  `service_ids` serialization already rides); anything else (wrong
  length, misplaced dashes, non-hex) is not an ACI and reads as absence.
  Absence is never repaired into a guess, and an unparseable face does
  not fail the finish or the sync — it is honest missing material, the
  same posture the §4.117 lanes take toward unseen vocabulary.
- Persistence: the account row gains a nullable `aci` column (schema
  23→24, additive migration, nothing backfilled — pre-1.142 rows
  legitimately hold none). The write face takes only a fresh answer: an
  absent or unparseable face writes nothing, so a stored identity
  survives every later face without the key — an engine that stops
  answering is not an engine that unlinked the identity — and a fresh
  answer overwrites.
- The wire: accounts.list entries and the link.finish result carry
  `aci` when the row holds one and skip the key entirely when it does
  not (no `aci: null` ambiguity). The schema leaves response results
  unpinned by design, so this section is the descriptive home for the
  shape.
- What deliberately does not change: the §4.42/§4.43 `isSelf` markers
  stay number-based (historical rows and the contacts ladder key on the
  number; the ACI is additive identity material for the desktop, not a
  re-keying of the self predicates), and the opaque row id keeps its job
  as the account's connector-side handle (`accountId` everywhere) — it
  is just never again the only identity-shaped material on the row.

Zero engine-side coupling beyond the two optional keys; no new method,
event, error, capability, or store move.

Tests: the canonical-shape ladder (the three answering shapes and the
refusal family — short, misplaced dashes, non-hex, empty), the writer
(persist, the None-never-unreports rule, the fresh-answer overwrite,
all three read faces), the v23→v24 migration (the column exists, the
migrated face reads as absence), the finish path (answered, absent, and
garbage faces through link.finish), the per-entry sync attribution
(each account carries its own answer, an absent entry stays absent
beside answered siblings), and the end-to-end harness (the answered
face riding link.finish and accounts.list with the pinned uuid5 value;
the omit-build reading as absence on both faces). Gates: fmt, clippy
`-D warnings`, tests all green, release build.

### 4.139 The rate-limit family joins the queue — on the Retry-After beat, not the curve (contract revision 1.143, 2026-10-13)

§4.123's deviation ledger kept the rate-limit family manual for a reason it
named itself: the backoff curve's 190 ms head would turn a server-ordered
pause into a hammer. The ledger's answer — leave the row manual — was the
narrower wrong fix: the desktop sees a dead failed row for a failure the
official client answers automatically, and the row's one manual retry walks
straight back into the same 429 the server is still counting. This revision
keeps the ledger's insight and fixes the mechanism: the pause the server
asked for IS the schedule.

- The classification face (contract 1.143): the funnel's one-bool "should
  the queue take this" answer grows into a three-arm decision
  (`SendRetrySchedule::None` / `Backoff` / `RateLimit`) — a 429/413 is a
  definitive engine answer (the server said "wait", not "lost"), so the
  unknown-outcome red line never applies, and the row enrolls like any
  queue member instead of dying manual. The proof/identity gates stay
  manual (they carry product judgments the funnel must not make silently —
  §4.123's ledger still owns them), and the structured-code families stay
  mixed-deterministic-manual.
- The beat: the official `sleepForRateLimitRetryAfterTime` waits the
  server's Retry-After value and falls back to `DEFAULT_RETRY_AFTER` =
  MINUTE when the answer carries none. This wire carries none by contract —
  the engine's rate-limit face is the class anchor `(RateLimitExceeded)`
  alone, no value field — so the official value-less fallback IS the beat:
  `RATE_LIMIT_RETRY_AFTER_MS` = 60 000 ms from the settlement instant,
  exactly, not a curve term. The official algorithm's other constants
  (the 1 s floor, the 24 h-window remainder clamp) govern a value face
  this wire does not have; the §4.123 window judges the row at claim time
  as always, which is the connector idiom for the same bound.
- Everything else about the row is the shared §4.123 face, verbatim: the
  eligibility gate (outbound text row, `clientRequestId`, no attachment
  descriptors §4.134), the attempt ceiling (128), the per-account queue
  ceiling (256), the window anchor and its claim-time judgment, the atomic
  settle-and-enroll write, and the drain. The rate-limit arm is a beat, not
  a bypass — a budget-spent row under 429/413 settles terminal exactly as
  under the curve. The §4.124 anchor stands: SendCommand.java carries no
  client-timestamp face, so the retry dispatches through the ordinary
  rebuild (same clientRequestId, same single row).
- The wire grows nothing: the schedule decision is process-internal (the
  settle face's enum replaces the bool it could not extend), the queue
  lives in the existing §4.123 columns, and no method, event, key, error,
  capability, or store move rides along. Zero engine-side coupling beyond
  the anchor the §4.80 classifier already pins.

Tests: the store's exact beat (the rate-limit enrollment lands at
`+RATE_LIMIT_RETRY_AFTER_MS`, never the curve's 190 ms head; the window
anchor stamps; the shared budget refusal holds on the new arm), the
three-arm classification (rate-limit → the Retry-After arm, transport →
the curve, everything else — proof/identity/structured-code/timeout/
unknown — plain `None`), the service enroll test's rate-limit leg (moved
out of the refusal family onto its own beat assertion, the window stamped
and the count bumped once), and the outbox end to end (the fixture's
one-shot rate-limit answer — the `-32603` + anchor face verbatim — fails
the send, enrolls the row at the one-minute beat, and leaves exactly one
row). Gates: fmt, clippy `-D warnings`, tests all green, release build.

## 5. signal-cli Boundary

The connector starts multi-account JSON-RPC mode without `-a`:

```text
signal-cli --data-dir <profile-private-data-dir> jsonRpc
```

Rules:

- executable path, data directory, JRE arguments, and environment come from a signed runtime
  manifest, never from an IPC request.
- stdin writes one JSON-RPC object per line.
- stdout is incrementally parsed with a hard line-size limit.
- response IDs are matched to a bounded pending map.
- cloud-backed list projections are capped connector-side so one host frame stays bounded: the
  §4.33 pinned-conversation list at 128 entries and the §4.34 sticker-pack sync list at 256
  entries — cloud order preserved, overflow truncated, entries missing the identity field
  dropped; the cloud record remains the source of truth and the connector persists neither list.
- view-once erasure (§4.37) covers only the channels this connector can see: the row body bytes
  in its store, the media files under the attachment directories it owns, and its own handle
  table. The engine's protocol store and any CDN blob are outside connector reach — the same
  receiving-side scope as the official view-once erase. No ViewOnceOpenSyncs staging: an open
  sync that arrives before its message is warn-only dropped, and the sender's resync cycle
  re-delivers it. `peer_identities` is bounded (one row per ACI, contacts scale); `senderUuid`
  capture applies from 1.38 forward and earlier rows fall back through the §4.37 ladder.
- `method=receive` is normalized as an event.
- stderr is redacted diagnostic input, never protocol input.
- read-only requests may be retried under policy; mutating requests are never retried automatically
  after an unknown outcome.
- a mutating request whose result is lost must not be reported as a retryable timeout. `finishLink`
  in particular answers `LINK_OUTCOME_UNKNOWN` with `retryable=false`, because the phone may already
  have accepted the device and a retry would claim a second device slot. A definite
  `UPSTREAM_TIMEOUT` stays retryable: the call provably did not take effect.
- child shutdown is graceful first, then forced after a fixed deadline.
- the engine does not read OS proxy settings. An optional SOCKS proxy is configured with the
  `KT_SIGNAL_SOCKS_PROXY=host:port` environment variable (preferred, so it stays off the process
  command line) or `serve --socks-proxy host:port`; the value is validated at launch, nothing
  proxy-shaped is placed on the child command line, and proxy parity holds at the environment
  level — the engine child inherits the same `KT_SIGNAL_SOCKS_PROXY` variable and maps it onto
  reqwest's `ALL_PROXY`, which the libsignal-service HTTP and WebSocket transport (a single
  reqwest client) honors natively. An invalid value aborts startup with a clear error. The
  default is a direct connection, and watchdog restarts reuse the same proxy config. Phase 4
  (ADR 0001) generalizes the configuration per proxy group (`KT_SIGNAL_PROXY_GROUPS` / repeated
  `serve --proxy-group`), the legacy global option configures the `default` group, and each
  group's watchdog restarts reuse that group's config unchanged.
- the engine (ADR 0003, 2026-10-10 — the only engine; the signal-cli JVM/native runtime modes and
  their selection flags are deleted) is KT's own AGPL-3.0 sidecar `kt-signal-engine` (separate
  repository; the libsignal-based replacement for the signal-cli JVM). `--signal-cli` points at
  the engine executable (flag name kept for launcher compatibility). The stdio JSON-RPC surface,
  the argv tail (`--data-dir <dir> jsonRpc --receive-mode on-start [--ignore-attachments]
  --ignore-stories --ignore-stickers`), and every supervision guarantee carry over unchanged, so
  the watchdog, unlink detection (§4.17: the engine reproduces the AuthorizationFailed
  classification surface), and shutdown semantics apply unchanged. `--java-home` is accepted and
  ignored with a warning (the desktop launcher still passes it; the desktop-side cleanup is a
  separate batch), the engine's argv parser is fail-closed on unknown arguments, and the child
  environment is inherited except for the store-key override. The engine writes its own
  `engine-state.json` / `engine-send-log.jsonl` / `accounts/lk-<hex>` layout under the group data
  directory, disjoint from the connector's `.kt-signal-connector.lock`. ~~`--media-ingest` is
  downgraded instead of refused (2026-10-03): the engine never downloads attachments, so arming
  ingest silently keeps the pre-1.17 shape — `--ignore-attachments` stays on the argv, the media
  governor is not armed, and every media method answers CAPABILITY_UNAVAILABLE — with a
  launch-time warning, so the dev desktop (which arms ingest unconditionally) still boots instead
  of failing closed.~~ **已由 `effective_media_ingest` 直通取代（契约 1.121, §4.102，本
  commit）**：launch 决策逐字直通——armed ingest 从 argv 摘掉 `--ignore-attachments`、武装
  media governor、握手按同一有效决策广告 `attachment-download`；取代理由与决策链见 §4.102
  （引擎 1.61 闭合后 receive 已投影 locator 面，§4.54 下载链按需取字节，降级的历史前提不再
  成立）。Runtime manifests describe the connector bundle only: libsignal rides the
  engine face, and the former signal-cli/JRE component entries and their packaging chain are
  gone.

During the local Phase 1 PoC, the trusted launcher supplies absolute executable and data-directory
paths as process arguments; neither is accepted over host IPC. Phase 3 replaces this bootstrap with
signed runtime-manifest verification before packaging acceptance.

## 6. Linking and Messaging

### 6.1 Linking

`link.start` calls signal-cli `startLink`. The raw device-link URI remains only in connector memory.
KT receives a short-lived session ID and the QR payload needed for display. `link.finish` resolves the
session ID internally and calls `finishLink`; KT cannot supply or alter the raw URI.

One proxy group permits one active link flow; different groups may hold independent link flows in
parallel (Phase 4, ADR 0001 — a single-group profile keeps the original global behavior). Within a
group, `link.finish` uses its own single-capacity wait lane so a
five-minute phone-approval wait cannot block runtime control, `link.start`, or `link.cancel`.
Duplicate finish calls are rejected instead of accumulating pending requests.

Expired sessions keep their attribution: a `link.finish` (or `link.cancel`) for a session that has
already expired is still routed to its owning group's wait lane and answered there with the
definite `LINK_EXPIRED` (`retryable=false`, matching every other definite finish failure) instead
of the unknown-session `LINK_NOT_FOUND`. Answering expiry consumes the session, so any later call
for the same id reports `LINK_NOT_FOUND`; only a session the connector no longer knows at all is
`LINK_NOT_FOUND` on the first attempt. If the owning group's runtime is stopped, the ordinary
`RUNTIME_NOT_RUNNING` answer precedes dispatch, as for any finish.

Per-engine account ceiling (optimization-plan §6.4 M3.3, decision D3): one proxy group's engine
serves at most 8 accounts. `link.start` on a full group is refused before the engine mints a QR,
and `link.finish` is refused before dispatching `finishLink`, so a phone is never asked to approve
a link the connector would then reject at commit; both answers are `ACCOUNT_LIMIT_REACHED`
(`retryable=false`) and name the group. The store-level guard re-checks at commit and treats
re-linking a number already bound to the full group as an update, not an addition. A pre-ceiling
data directory that already holds more accounts keeps serving them: engine re-sync imports
reality and never truncates.

Cancellation is linearized before it returns: a cancelled or superseded finish result cannot create
an account row or emit `account.changed`. The pinned signal-cli JSON-RPC API has no operation that
cancels an already-dispatched `finishLink`; when `link.cancel` finds one in flight, the connector
restarts that link session's group engine after clearing the link session. This does not delete or
unlink existing accounts, but it can briefly pause every Signal account bound to that group (in a
single-group profile, every account in the profile).
KT must therefore reuse an unexpired QR and perform this restart only for an explicit replacement,
not an automatic render retry.

Cancellation, timeout, client disconnect, or connector shutdown zeroizes and removes the state. QR
data is never stored or logged.

### 6.2 Local account exit

`accounts.deleteLocalData` accepts an `accountId`; current KT versions also send a stable generated
`operationId`. The field remains optional in API 1.0 so a Connector upgrade does not break an older
Desktop, but new Desktop code must persist and reuse it. The operation removes only that account's
signal-cli data and Connector rows; it must not clear or replace the profile's active link session.
Connector row deletion is one SQLite transaction and is idempotent when the account row is already
absent.

For requests carrying `operationId`, the Connector persists only the random operation ID, opaque
account ID, state, and timestamps. It does not retain the Signal address after account deletion.
At most 256 completed operations are retained; one unfinished operation is allowed per account. On a
later explicit user retry, an unfinished operation first performs two read-only `listAccounts`
checks. If both confirm that the account is absent, the Connector atomically completes its local
cleanup without issuing a second delete; otherwise that user action may dispatch the delete again.

Delete completion is a store-level fact, and a replay must not depend on the owning proxy group
being reachable (Phase 4, ADR 0001). Routing consults the account row only to find the owning
engine; when the row is already gone — an idempotent replay, or a v1-compatible delete of an
absent account — the shared operation ledger completes the operation entirely inside the store,
served through any supervisor (all groups share one store) without contacting any engine, even
when the account's group is not part of this launch plan. Conversely, a first attempt interrupted
between upstream deletion and local commit leaves the row in place: the replay routes normally to
the owning group, where the reconcile-first check above completes locally once the upstream no
longer lists the account; re-dispatching the upstream delete there is safe because it is
idempotent. Replay therefore never requires the owning group to be alive and never resurrects
local rows on its own.

The Connector does not retry `deleteLocalAccountData` automatically. If signal-cli returns a
definitive failure, local rows remain unchanged. If the process, transport, or timeout makes the
result unknowable, the response is `ACCOUNT_DELETE_OUTCOME_UNKNOWN` with `retryable=false` and local
rows also remain. KT keeps the binding and the same `operationId`; only a new explicit user action may
repeat the idempotent exit operation. This avoids both silent local data loss and background repeats
of a destructive operation.

### 6.3 Receive

```text
signal-cli receive notification
  -> bounded JSON parser
  -> schema validation + bounded projection
  -> critical receive queue (bounded by count and bytes)
  -> normalize + stable message ID
  -> SQLite transaction + dedupe
  -> best-effort UI event queue
  -> KT event
```

The critical receive queue is independent from the lossy runtime/UI broadcast queue. Persistence of
the message, conversation summary, and account unread summary is one transaction and always happens
before event delivery, so a lagging or disconnected KT event consumer reloads facts from the store.
If SQLite is unavailable, the Connector retains the current receive and applies bounded
backpressure instead of acknowledging it internally or silently dropping it. It emits only an
opaque `runtime.storageChanged` state (`unavailable` or `recovered`); Desktop pauses new operations
while unavailable. Recovery retries persistence with bounded backoff and never replays a send,
restarts signal-cli, deletes history, or exposes a path or message body.

Incoming text is bounded twice after signal-cli parsing: Connector persistence accepts at most
128 KiB, matching Signal iOS's legacy-compatible receive ceiling, while list/event projections carry
at most a 4 KiB UTF-8 preview. Larger upstream bodies retain a stable message row, bounded preview,
original byte count, and an explicit non-retrievable marker; the excess body is discarded before
SQLite and Host event serialization. A user may fetch one complete persisted body through
`messages.getText`; bulk/background fetch is not exposed.

### 6.4 Send

Every send has a KT `clientRequestId`. The connector inserts a pending record before calling
signal-cli. A repeated request returns the same local record. Timeout or child death after dispatch
produces `unknown`; it never automatically sends again.

`clientRequestId` is persisted and returned as an optional field on that outgoing message so KT can
reconcile an optimistic row and an unknown outcome exactly. It is not forwarded to Signal and is
never used across accounts. Renderer and Connector must not reconcile by equal text: repeated text
is valid user data. Signal receive dedupe uses the account, conversation, direction, sender, and the
Signal-provided message timestamp; before inserting a new versioned stable ID, the store also checks
that identity tuple so rows written by the legacy text-dependent ID remain idempotent.

Receive normalization preserves the exact Signal text body, including leading/trailing whitespace.
Only an empty string is body-less. A body-less envelope becomes a system row only for an explicit
supported Signal control field; it is never guessed from the absence of attachments. `system`
direction and status round-trip through SQLite unchanged.

Text must be non-empty and at most 65,536 bytes after UTF-8 encoding. This follows the 64 KiB
long-message body cap used by the official Signal clients, while signal-cli handles the native
inline-versus-long-text attachment representation. The JSON schema character limit is only an early
shape guard; the Rust service byte check is authoritative for CJK, emoji, and malformed input.
Oversized text is rejected before creating the pending row or dispatching signal-cli and is never
silently truncated, split, or automatically retried.

Reference implementations (pinned links; no source is copied into this repository):

- [Signal Desktop `longAttachment.std.ts`](https://github.com/signalapp/Signal-Desktop/blob/de8fe1e7084fbab9c4e9c667c2d0ec0f208d1adc/ts/util/longAttachment.std.ts)
- [Signal Android `MessageUtil.kt`](https://github.com/signalapp/Signal-Android/blob/9b2c2ed66d854b7abb8ed1a29e976a516ab2ce67/app/src/main/java/org/thoughtcrime/securesms/util/MessageUtil.kt)
  and [`ByteLimitInputFilter.kt`](https://github.com/signalapp/Signal-Android/blob/9b2c2ed66d854b7abb8ed1a29e976a516ab2ce67/core/util/src/main/java/org/signal/core/util/ByteLimitInputFilter.kt)
- [Signal iOS `OWSMediaUtils.swift`](https://github.com/signalapp/Signal-iOS/blob/58cc49ec14da01e7afa89d6e603ba1ca79bcf9b4/SignalServiceKit/Messages/Attachments/OWSMediaUtils.swift)

AI-generated and human messages use the same send method. Language policy and tenant capability are
enforced by KT before the connector call, while connector ownership and idempotency checks remain
mandatory.

### 6.5 Contacts cache and peer-addressed send

After link, signal-cli has already synchronized contacts and groups from the primary device.
`contacts.sync` reads them via read-only `listContacts` (registered contacts only, no
`allRecipients` walk) and `listGroups` (membership-filtered, `isMember=true`) on the account's
group engine queue and upserts them into a per-account `contacts` cache table keyed by
`(account_id, kind, peer_key)`. A successful sync less than 60 seconds old returns the cached
counts without touching the engine. `link.finish` runs one best-effort sync inline; its failure is
logged and never fails the link flow. Every group supervisor engine start (boot `runtime.start` or
a watchdog recovery restart) also runs one best-effort background re-sync pass for the group's
linked accounts after a settle delay: an account linked before the skeleton revision — or whose
inline link-time sync failed — still materializes its contact/group conversation skeletons on the
next engine start instead of surfacing conversations only message by message. The pass is
one-shot per process start (history-retention discipline), warn-only, and the 60-second cache
folds repeat starts into read-only no-ops. `contacts.list` serves the cache only (optional substring
filter, cursor pagination) and never calls upstream, so the Desktop new-chat picker cannot starve
the JVM queue. Contact rows and the sync marker are deleted with the account.

`messages.sendText` accepts either `conversationId` or a peer target (`kind` + `peerKey`, optional
`peerTitle`); exactly one form is required. A peer send resolves the conversation by
`(account_id, kind, peer_key)` and, when absent, creates it together with the first outgoing
message. `kind` accepts `contact`/`direct` (both a direct chat) or `group`; a missing `peerTitle`
falls back to the masked peer address, never the raw number.

Contract revision 1.14 (2026-09-23) replaces the earlier "no empty conversation skeletons"
resolution: a successful `contacts.sync` now also ensures one conversation row per synced
direct contact and per synced member group, in the same transaction as the contact upserts.
This mirrors the official Desktop post-sync shape — the linked account sees its existing chats
as empty skeletons (sorted after conversations with history by `last_message_at IS NULL`), the
title comes from the sync cache and upgrades through the normal `title_should_upgrade` path
when the first real message lands. Message history is still never backfilled (§6.6); a skeleton
only becomes an active conversation through a real message. The pre-1.14 behavior —
conversations created only by the first sent or received message — remains true for entries the
upstream has not synced (unknown numbers that message the account create their conversation on
arrival as before). Since contract revision 1.39 the other half of the skeleton story is the
link-time history import (§4.38, §6.6): a skeleton conversation is never duplicated by the
import — the archive's chats resolve to the same `(account, kind, peer_key)` identity and fill
the existing row in place through `ensure_conversation`, upgrading the title through the normal
policy when the archive knows a better name.

### 6.6 History limitation and link-time import (backup5)

The connector only promises history it has persisted. `sendSyncRequest` can synchronize contacts and
groups but is not treated as complete phone/Desktop message-history backfill. Product UI must not
promise pre-link history beyond what the import below actually lands. Since contract
revision 1.14 the synced chats do surface as empty conversation skeletons (§6.5); their message
lists stay empty until real messages arrive or the link-time import fills them.

Verified against official sources (2026-10-03, Signal-Desktop v8.31.0-alpha.1): the mechanism is
the `backup5` link capability (`ts/textsecure/Provisioner.preload.ts` — advertised in the link QR
URL only when the desktop has never registered) plus a `ProvisionEnvelope.ephemeralBackupKey`
carried by the phone at scan time; the newly linked desktop then downloads a full backup archive
from the Signal backup service and imports it before entering the app
(`ts/services/backups/index.preload.ts`). Two product constraints apply officially as well: the
phone must be online and cooperate at link time, and an installation that ever registered cannot
re-import (isLinkAndSyncEnabled gates on registration). Upstream tracking: AsamK/signal-cli
issue/PR #2134 (open, 2026-09-29, "qr history daemon") proposes `importHistory` on
`startLink`/`finishLink` plus paged `export-history` with attachment metadata only — not merged,
not released, and has no maintainer engagement. Product decision (2026-10-03): full link-time
history is a hard requirement, so this is no longer a standing boundary but an active project:
kt-signal-engine ADR 0005 (2026-10-03) commits to a self-built backup5 receiver (capability
advertisement, ephemeralBackupKey capture, archive download/decrypt, receive-compatible frame
export); libsignal v0.99.0 (already in the engine's dependency tree) ships the `message-backup`
and backup-key primitives. The S1 kill-gate spike PASSED on 2026-10-03 (ADR 0005 §取证记录):
the phone honors a third-party `backup5` QR capability and the provisioning envelope carries
`ephemeralBackupKey`, `GET /v1/devices/transfer_archive` plus the CDN attachment download
succeeded byte-level (6,576-byte archive), and no presage patch is needed. S2 landed in the
engine on 2026-10-04 (kt-signal-engine 0eedc8c, ADR 0005 §5): the link flow bypasses presage's
`Manager::link_secondary_device` (which drops `ephemeral_backup_key`) and calls
`provisioning::link_device` directly with `capabilities=backup5` on the QR URL, persists the
registration through the official store format, then best-effort long-polls the transfer
archive, decrypts it (official `libsignal-message-backup` v0.99.0 primitives) and streams a
frame-by-frame NDJSON export to the account store directory (`history-import.ndjson`).

**Contract revision 1.39 (2026-10-04) consumes that face (§4.38)**: the connector imports the
archive into its store after link — bounded batched writes, live-traffic identity dedupe
(same `signal-message-v2` stable id, `sent_at` = the archive's Signal timestamp, so history and
live envelopes of the same message collapse to one row), existing §6.5 skeletons filled in place
through `ensure_conversation`, no unread-badge or host-event flood, retention converged by the
normal §7.1.1 pass. The import is best-effort and honest end to end: the desktop reads
`history.importStatus` (§4.38) — `unavailable` under signal-cli modes (pinned signal-cli 0.14.x
has no such capability; the kt-engine mode is the only import-capable face) or when no archive
was produced, `pending` during the link-time wait, `running`/`completed`/`failed` from the
persisted row with real counters. Attachments stay metadata-only (§6.7 philosophy — the archive
carries no bytes and the connector never downloads any). The archive itself is never mirrored in
full: imported rows live under the same §7.1.1 retention bounds as every other stored message
(age measured on local `stored_at`, per-conversation cap converged right after import), so the
product promise is "the phone's recent history, to the connector's normal retention bounds", not
a full-archive shadow. Engine-side dependency registered for the engine's S3 task: the S2 export
does not yet project recipient service identity (`aci`/`e164`/`masterKey`), so against the
current engine build a run completes with every chat skip-counted; the connector consumes the
fields the moment the engine adds them (no further connector change).

**Contract revision 1.80 (2026-10-08) adds the manual snapshot arm (§4.68)**: the same import
pass is now reachable without a phone at all — the desktop hands the engine a user-selected
official Signal Desktop snapshot directory (`backups.importSnapshot`), the engine verifies it
against the account's own key material and streams the same `history-import.ndjson`, and the
connector drives the identical §4.38 pass (one ledger row, the same
`history.importStatus`/`history.importRetry` faces, the same retention bounds and dedupe keys).
The §4.38 honesty rules are inherited whole: the engine's failure modes land as failed rows with
`error_class = "engine"` and the retry face answers NO_ARCHIVE until a snapshot is imported
again. What this restores is bounded by the same §7.1.1 retention bounds as every other stored
message — "what the official desktop backed up, to the connector's normal retention bounds",
not a full-archive shadow.

**Contract revision 1.81 (2026-10-08) lands the seven non-text backup5 row types (§4.38)**:
frame-level `adhocCall` call records and the ChatItem `payment`/`giftBadge`/`viewOnce`/
`storyReply`/`poll`/`adminDeleted` faces. The connector consumes the engine's live-key
projections — view-once through the existing receive pathway untouched, the rest through the
additive rich channel — with the aggregation and resolution the official import assigns the
consumer: poll votes fold per voter, the story author resolves from direction and
conversation, the admin tombstone flips the 1.33 flag. What does not cross is recorded, not
invented: payment amounts/fees/transaction blobs, the gift-badge credential presentation,
the poll voteCount tally, the storyReaction emoji face, the admin identity, and the call
records themselves (no call-history store) all have no KT row face today. The desktop
renders what arrives (67abb977); rows the archive carries but KT cannot express honest-skip.

### 6.7 Media limitation

Phase 1 runs signal-cli with attachments, stories, and stickers ignored. Current signal-cli downloads
non-ignored incoming attachments before it emits the receive notification, and `getAttachment`
returns an already-downloaded file as full Base64. That is not an acceptable large-media boundary.

Media remains ~~disabled until a dedicated PoC proves bounded disk-pressure behavior and streams a
canonical, already-downloaded file through a short-lived handle without Base64 or arbitrary path
exposure. Failure to prove the limit leaves the media capability disabled.~~ **已由
`effective_media_ingest` 直通取代（契约 1.121, §4.102，本 commit）**——the bounded-download PoC
passed its acceptance gates (the 1.17 chunked channel, the 1.57/1.61 transit-CDN chains, the
quota+TTL media governor) and the launcher's `--media-ingest` decision now passes through
verbatim; this paragraph is the historical metadata-only red line the flip retires, kept for the
decision chain (取代理由与决策链见 §4.102).

Contract revision 1.15 (2026-09-23) narrows this boundary without lifting it: incoming envelopes
now contribute bounded attachment **metadata only** (§4.13) — descriptors are stored on the
message row while `--ignore-attachments` keeps byte download, disk pressure, and
`messages.attachments.get` answering `UPSTREAM_ERROR` exactly as before. A media capability
change still requires the dedicated bounded-download PoC this section demands.
(2026-09-24, contract 1.20: the one-shot `messages.attachments.get` reader itself was removed
after the PoC landed — §4.16; the boundary statement now lives entirely in the chunked
channel's gating.)

That PoC is now designed: `docs/adr/0004-media-ingest-poc.md` (2026-09-23) defines the
opt-in `--media-ingest` spawn flag, a quota+TTL media governor, and chunked handle
delivery (`messages.attachments.open` / `readChunk` / `closeHandle`, contract revision
1.17). ~~Until that PoC passes its acceptance gates, this boundary stands.~~（该门已过、
红线已由契约 1.121 §4.102 的直通取代——同段上方指针。）

## 7. Data and Resource Limits

### 7.1 Data ownership

| Data | Owner | Cleanup |
| --- | --- | --- |
| Signal keys and protocol account DB | signal-cli private data directory | explicit destructive action only |
| normalized messages, conversations, contacts, cursors | connector SQLCipher store | retention pass, see 7.1.1 |
| plaintext migration backup | connector store directory | 7-day retention, see 7.1.2 |
| current 100-200 message window | KT UI | release on navigation/unmount |
| media cache | connector | TTL/LRU after media approval |
| link URI | connector memory | timeout/cancel immediately |

The signal-cli data directory is never a cache and must be explicitly excluded from cleanup code.

### 7.1.1 Message retention

Stored history is bounded so a long-lived install cannot grow without limit. One retention pass runs
per process start, in the background, and deletes messages that are both outside a safety window and
selected by one of two rules: older than 90 days, or beyond the newest 2000 rows of their
conversation. It never repeats on a timer; whatever is left is expired on the next start.

- Age is measured on `messages.stored_at`, the local time this connector first stored the row, never
 on `sent_at`. A peer with a wrong clock must not be able to decide when local history disappears.
 Rows written before schema 6 have no `stored_at` and are dated by `received_at` instead; the upgrade
 deliberately does not rewrite the table, because that would delay the startup handshake.
- Nothing inside a 7-day floor is pruned, and neither is any send whose outcome is still `pending` or
 `unknown`. Receive dedupe and send idempotency both answer from stored rows.
- Conversations, contacts and accounts survive an empty history, so titles, pins and the link itself
 are never lost to retention. Summaries of conversations that lost rows are recomputed in the same
 transaction, and an unread badge is clamped to the incoming rows still stored.
- Each transaction deletes a bounded batch and releases the store lock between batches, so a large
 first pass cannot stall inbound receives.
- Deleted pages do not break a caller mid-scroll: message cursors carry their own `(sent_at, id)`
 sort key, so a page still resolves after the row it pointed at is gone.

### 7.1.2 Store encryption at rest

The connector store is SQLCipher (rusqlite `bundled-sqlcipher`), keyed with the 32-byte store key
from the bootstrap payload; the blast radius of the encryption boundary is `Store::open`. A store
file starting with the plaintext `SQLite format 3` header is migrated on first start: the plaintext
WAL is checkpointed, one consistent plaintext backup (`connector.sqlite3.plaintext-backup`) is
written for rollback, the data is exported online into a fresh encrypted file via
`sqlcipher_export`, and the encrypted file is atomically swapped in. Migration failure fails closed
— the connector never silently serves plaintext — and no plaintext copy lands anywhere but the
backup. The backup is pruned after 7 days through the same once-per-start retention pass as
history (7.1.1). A keyed open that the store rejects fails closed as well.


### 7.2 Memory budget before measurement

| Component | Idle RSS | Active text RSS | Media transient RSS |
| --- | ---: | ---: | ---: |
| signal-cli JVM/native | 140-280 MB | 200-400 MB | 300-600 MB |
| Rust connector | 8-25 MB | 15-40 MB | 20-60 MB |
| KT Signal gateway/UI/cache increment | 20-70 MB | 35-110 MB | 60-150 MB |
| total Signal increment | 170-350 MB | 250-550 MB | 400-800 MB |

These are capacity budgets, not measurements or promises. Phase 3 records actual process RSS on each
target platform and replaces the estimates.

Additional idle account budget: 20-80 MB while sharing the same JVM. Each additional proxy group
(Phase 4, ADR 0001) adds one full engine at 140-280 MB idle RSS; the group count is capped at 8 by
the connector and at a product default of 4 by desktop policy, so the worst-case idle Signal
increment is bounded by configuration rather than by account growth. An ordinary open text
conversation should normally remain below 1-5 MB incremental UI state, with an acceptance hard limit
of 20 MB relative to the Signal idle baseline.

The Connector enforces the initial JVM budget at `-Xms16m -Xmx384m` and removes Java's global
option-injection variables before spawning signal-cli. JVM heap is not total RSS. Sustained idle
Signal increment above 350 MB or monotonic growth triggers profiling rather than a documentation
increase. RSS pressure still degrades admission and never kills a live JVM automatically.

### 7.3 Bounded structures

- host frame: default 1 MiB, configurable only downward in production policy.
- signal-cli line: default 8 MiB for text PoC; media does not use this path.
- host connections: exactly one successfully authenticated KT Main connection per Connector process.
  Failed handshakes do not consume the process; closing the authenticated session shuts down the
  engine and Connector, and reconnection starts a fresh process with a fresh one-use secret.
- pending host requests: 128 global, 32 per account, and 8 MiB aggregate request bytes per
  authenticated connection.
- authenticated host dispatch: control 1, link wait 1, persisted reads 4, sends 2; sends remain
  ordered per account and responses are correlated by `requestId`, not arrival order. The link-wait
  lane admits at most one `link.finish` per proxy group and is independent from link cancellation
  and lifecycle control.
- pending signal-cli requests: 128 per group engine (Phase 4: each proxy group's engine keeps its
  own bounded queue; host-side limits stay global on the single authenticated connection).
- per-engine accounts: at most 8 per proxy group (decision D3, optimization-plan §6.4 M3.3),
  enforced at both link entries with `ACCOUNT_LIMIT_REACHED`; pre-ceiling data directories keep
  serving existing accounts and re-sync never truncates.
- runtime/UI broadcast queue: 1,024 non-critical events with pressure reporting; lag is recoverable
  from SQLite.
- critical receive queue: 256 items and 2 MiB of normalized projected data. It backpressures the
  signal-cli stdout reader at either limit and never routes receives through broadcast delivery.
- current message page: client-supplied `limit` is required in schema (1–200; the store clamps to the same range) and has no server-side default.
- message text projection: 4 KiB per list/event row; persisted inbound body: 128 KiB maximum.
- one signal-cli RSS sampler per group engine, every 30 seconds. Pressure requires three consecutive
  samples at or above 512 MiB; recovery requires two consecutive samples at or below 420 MiB.
  Sampling emits only PID/RSS/state and exits immediately with the engine. RSS pressure never kills
  or restarts the JVM automatically. Pressure events identify their group by `groupId` (see 4.4).
  The 60-second metrics snapshot additionally logs each engine's latest RSS sample (group id and
  bytes, one line per sampled engine) next to the queue gauges (`receiveQueueDepth`,
  `hostPendingRequests`) and counters (`receiveDroppedTotal`, `watchdogRestartsTotal`), so a soak
  run's drop/restart evidence is in the connector log without any external metrics service.
  macOS/Linux sample through a short-lived `ps`; Windows uses the
  native process working-set API and never starts PowerShell for monitoring.
- conversation cursors are opaque keyset cursors over `(last_message_at nullness,
  last_message_at, id)`; message cursors are opaque keyset cursors over `(sent_at, id)` bound to one
  account and conversation, so a page still resolves after retention removed the row it pointed at.
  Unknown, malformed or cross-account/cross-conversation cursors fail closed instead of silently
  returning the first page. A bare message id is still accepted, for cursors a running host obtained
  before the connector was upgraded.
- reconnect attempts: exponential backoff with a circuit breaker.

Desktop may restart a terminal engine only after its bounded request scheduler drains, with no
request replay and a three-attempt circuit breaker. If an active mutation does not drain, recovery
fails closed for explicit user action; resource pressure alone is not a restart trigger. Terminal
events carry the exited PID so a delayed event cannot fault a newer engine, and an explicit Desktop
stop always wins a concurrent recovery completion. A clean internal `stopped` to `running` transition
is treated as a managed restart and must not trigger a second engine restart.

Core persistence must not be dropped under event pressure. Non-critical enrichment is disabled
first. SQLite write failure enters an explicit storage-degraded state; one failed receive remains at
the head of the bounded queue until a transaction succeeds, after which a recovered state is emitted.

## 8. Security and Privacy

- Owner-only endpoint, secret file, account data, SQLite, and diagnostic directories.
- No secret in command-line arguments, environment logs, telemetry, or crash reports.
- Runtime schema and length validation at every IPC and JSON-RPC boundary.
- Canonical-path checks for every approved attachment file handle.
- No message body, contact, phone number, QR payload, token, key path, or raw envelope in logs.
- Metrics use opaque account IDs, method categories, sizes, durations, result classes, and trace IDs.
- Connector API is default deny; a manifest capability does not authorize an operation by itself.
- Deleting local account data is separate from unlinking and requires explicit confirmation in KT.

## 9. Licensing and Distribution

- This connector is AGPL-3.0-only and remains an independent executable.
- signal-cli remains an independent, unmodified GPLv3 executable.
- libsignal remains under Signal's AGPLv3 license inside the signal-cli runtime boundary.
- KT Desktop does not copy or link connector, signal-cli, or libsignal code.
- The host protocol is public, versioned, general-purpose, and does not exchange internal language
  objects or shared memory.

Every binary release must include or provide equivalent access to the exact corresponding source,
build scripts, license texts, notices, SBOM, source commits, hashes, and patch list for all covered
components. Pointing only to a moving upstream branch is insufficient. Phase 1 patch lists must be
empty for signal-cli and libsignal.

This code license does not grant Signal service, API, trademark, or partnership authorization. The
product must describe the integration as unofficial and separately review Signal service terms,
automated messaging behavior, account suspension, and compatibility risk.

## 10. Phases and Exit Criteria

### Phase 0: repository and delivery contract

Deliver:

- Rust project, AGPL license, NOTICE, README, agent rules, and this plan.
- local build, format, test, clippy, and release build.
- local commit only.

Exit: a clean review finds no contract contradiction, secret, unrelated file, or build failure.

### Phase 1: process and protocol PoC

Deliver:

- authenticated private IPC on the current platform with platform abstractions.
- bounded host framing and API handshake.
- signal-cli child process supervisor and bounded JSON-RPC transport.
- runtime status/start/stop and normalized receive events.
- fake signal-cli integration fixture covering success, malformed output, timeout, crash, restart,
  shutdown, duplicate ID, and unknown send outcome.

Exit: fmt, unit/integration tests, clippy, release build, clean review, and local commit.

The macOS implementation and tests use a private Unix socket. The Windows implementation now uses a
random per-start named pipe with a current-user-only DACL, rejects remote clients, protects the
profile directories, bootstraps through inherited stdin, and watches the Electron parent so an
orphan connector shuts down its JVM. The Windows-only Rust modules compile against
`x86_64-pc-windows-msvc` in an isolated target check; native build and runtime acceptance on Windows
hardware are still required before packaging. Cross-compilation alone is not an acceptance claim.

### Phase 2: account linking and text channel

Deliver:

- start/finish/cancel link state with expiry and zeroization.
- account list and account-ID mapping.
- normalized direct/group text events and text send.
- SQLite schema, migrations, stable IDs, dedupe, pagination, unread, and client-request idempotency.

Exit: fake integration coverage plus a separately authorized real-account test. No real-account result
may be inferred from a fixture.

### Phase 3: packaging and resource baseline

Local hardening status (2026-08-09): the signed manifest, exact file verification, corresponding
source/compliance gates, immutable stage/atomic pointers, production verification CLI and fixed JRE
plumbing are implemented and automatically tested. Release keys, signed target bundles, native
Windows packaging/runtime, and the long-duration resource gates below remain open; this phase is
therefore not marked production-complete.

Deliver:

- signed connector, signal-cli, and JRE manifests for Windows x64 and macOS x64/arm64.
- exact source archive, LICENSE/NOTICE/SBOM, hashes, and reproducible build record.
- LKG stage/activate/rollback.
- idle, active text, restart, 24-hour, and multi-account resource reports.

Exit: target-platform resource gates and license delivery review pass.

### Phase 4: KT Desktop integration

Local integration status (2026-08-09): integrated in the separate `kt-desktop` worktree on
`codex/signal-test-main-latest`. Main and Connector use the same per-start endpoint, Main performs a
bounded connect retry instead of testing named-pipe existence, and eager challenge frames cannot be
missed during listener setup. This does not satisfy the remaining native Windows,
signed-distribution, 24/72-hour, or real multi-account production gates.

Deliver in KT Desktop:

- ConnectorSupervisor and connector client.
- native Signal account/conversation/message/composer UI.
- AI language policy, translation, remarks, and quick replies through the shared send path.
- tenant gray rollout and diagnostics.

Exit: desktop contract, integration, actual UI, and target-platform acceptance pass.

### Phase 5: optional media

Deliver only when proven:

- bounded incoming download and disk-pressure behavior.
- streaming file handles without Base64 or arbitrary paths.
- cache cleanup, cancellation, low-disk, and large-file tests.

Exit: resource/security review. Otherwise media remains disabled.

### Phase 6: optional Feature Host

This phase requires evidence of independent release, third-party extension, or fault-isolation need.
It reuses the connector and signal-cli processes and adds a bounded Feature Host pool; it never starts
one worker per conversation or one JVM per account.

## 11. Review Gates

Every phase follows:

```text
implement
  -> focused tests
  -> integration/build checks
  -> security/concurrency/resource review
  -> minimal fixes
  -> retest
  -> rereview
  -> local commit
```

Review priority:

1. wrong-account/wrong-recipient actions, key exposure, arbitrary execution, path traversal, data loss.
2. duplicate sends, unknown-send retry, races, deadlocks, orphan processes, unbounded memory/disk.
3. contract drift, malformed upstream data, restart behavior, diagnostics, missing tests.
4. maintainability and clarity that prevent future boundary mistakes.

No push, remote repository creation, release, deployment, or external message is part of these phases
without separate authorization.
