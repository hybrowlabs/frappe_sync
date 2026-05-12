import frappe
from frappe import _

from frappe_sync.frappe_sync.utils import get_conflict_strategy, get_sync_settings, get_sync_fields_for_doctype


@frappe.whitelist()
def ping():
	"""Health-check endpoint. Returns this site's site_id."""
	settings = get_sync_settings()
	return {"site_id": settings.site_id, "status": "ok"}


@frappe.whitelist()
def receive_sync(doc_data, event, origin_site_id, modified_timestamp):
	"""Receive a sync payload from a remote Frappe instance.

	Args:
		doc_data: JSON string of the document data
		event: "Insert", "Update", or "Delete"
		origin_site_id: The site_id of the instance that originated this change
		modified_timestamp: The modified value from the source doc
	"""
	try:
		frappe.flags.in_frappe_sync = True

		if isinstance(doc_data, str):
			doc_data = frappe.parse_json(doc_data)

		doctype = doc_data.get("doctype")
		name = doc_data.get("name")

		# Remove sync-specific metadata before processing
		dependencies = doc_data.pop("_dependencies", [])

		log = _create_sync_log(doctype, name, event, "Incoming", origin_site_id, modified_timestamp)

		# Resolve dependencies first
		_resolve_dependencies(dependencies, origin_site_id)

		if event == "Insert":
			_handle_insert(doc_data, log)
		elif event == "Update":
			_handle_update(doc_data, modified_timestamp, log)
		elif event == "Submit":
			_handle_submit(doc_data, log)
		elif event == "Cancel":
			_handle_cancel(doc_data, log)
		elif event == "Delete":
			_handle_delete(doctype, name, log)

		frappe.db.commit()
		return {"status": "ok"}

	except Exception as e:
		frappe.db.rollback()
		_create_sync_log(
			doc_data.get("doctype") if isinstance(doc_data, dict) else "",
			doc_data.get("name") if isinstance(doc_data, dict) else "",
			event,
			"Incoming",
			origin_site_id,
			modified_timestamp,
			status="Failed",
			error=frappe.get_traceback(),
		)
		frappe.db.commit()
		frappe.throw(_("Sync failed: {0}").format(str(e)))

	finally:
		frappe.flags.in_frappe_sync = False


@frappe.whitelist()
def get_changes_since(since_timestamp):
	"""Return recently modified documents for all synced doctypes.

	Called by remote sites in Pull mode to fetch changes they missed.
	Returns up to 100 changes ordered oldest-first so the caller can
	track progress with last_pull_at.
	"""
	from frappe_sync.frappe_sync.utils import get_sync_settings, prepare_doc_payload

	settings = get_sync_settings()
	changes = []

	for row in settings.synced_doctypes:
		if not (row.sync_insert or row.sync_update):
			continue

		docs = frappe.get_all(
			row.doctype_name,
			filters={"modified": (">", since_timestamp)},
			fields=["name", "modified"],
			order_by="modified asc",
			limit=100,
		)

		for d in docs:
			try:
				doc = frappe.get_doc(row.doctype_name, d.name)
				payload = prepare_doc_payload(doc, "Update")
				changes.append({
					"modified_timestamp": str(d.modified),
					"doc_data": payload,
				})
			except Exception:
				frappe.log_error(
					title="Pull Sync Payload Error",
					message=frappe.get_traceback(),
				)

	changes.sort(key=lambda x: x["modified_timestamp"])
	return changes[:100]


@frappe.whitelist()
def get_deletions_since(since_timestamp):
	"""Return deletion events logged since timestamp for pull-mode clients."""
	logs = frappe.get_all(
		"Sync Log",
		filters={
			"event": "Delete",
			"direction": "Outgoing",
			"creation": (">", since_timestamp),
		},
		fields=["doctype_name", "document_name", "creation"],
		order_by="creation asc",
		limit=100,
	)
	return logs


@frappe.whitelist()
def get_document(doctype, name):
	"""Fetch a document for dependency resolution by a remote instance."""
	if not frappe.db.exists(doctype, name):
		frappe.throw(_("Document {0} {1} not found").format(doctype, name))

	doc = frappe.get_doc(doctype, name)
	return doc.as_dict()


def _handle_insert(doc_data, log):
	"""Handle an incoming insert event using direct DB writes to bypass all controller hooks."""
	doctype = doc_data.get("doctype")
	name = doc_data.get("name")
	docstatus = doc_data.get("docstatus", 0)

	if frappe.db.exists(doctype, name):
		# If local doc is submitted/cancelled — skip completely
		local_docstatus = frappe.db.get_value(doctype, name, "docstatus")
		if local_docstatus in (1, 2):
			log.db_set("status", "Skipped")
			log.db_set("error", f"Skipped: {doctype} {name} already exists and is submitted/cancelled (docstatus={local_docstatus})")
			return
		_handle_update(doc_data, doc_data.get("modified"), log)
		return

	# Skip incoming submitted/cancelled documents
	if docstatus in (1, 2):
		log.db_set("status", "Skipped")
		log.db_set("error", f"Cannot insert submitted/cancelled {doctype} {name} (docstatus={docstatus})")
		return

	# Build scalar fields only (child tables handled separately)
	scalar_data = {k: v for k, v in doc_data.items() if not isinstance(v, list) and k != "docstatus"}

	new_doc = frappe.new_doc(doctype)
	new_doc.update(scalar_data)
	if name:
		new_doc.name = name
		new_doc.flags.name_set = True
	# Direct DB insert — bypasses all controller hooks (before_validate, validate, etc.)
	new_doc.db_insert()

	# Sync child tables directly
	_sync_child_tables(doctype, name, doc_data)

	# Set docstatus via db.set_value to avoid triggering on_submit/on_cancel side effects
	if docstatus in (1, 2):
		frappe.db.set_value(doctype, name, "docstatus", docstatus, update_modified=False)

	log.db_set("status", "Success")


def _handle_update(doc_data, modified_timestamp, log):
	"""Handle an incoming update event.
	- Doc exist nahi karta: insert karega
	- Draft doc (docstatus=0): ALL fields override honge
	- Submitted doc (docstatus=1): ONLY fields that are BOTH Allow on Submit AND in Sync Settings
	- Cancelled doc (docstatus=2): SKIP
	"""
	doctype = doc_data.get("doctype")
	name = doc_data.get("name")

	if not frappe.db.exists(doctype, name):
		_handle_insert(doc_data, log)
		return

	local_docstatus = frappe.db.get_value(doctype, name, "docstatus")

	# Cancelled doc — fully skip
	if local_docstatus == 2:
		log.db_set("status", "Skipped")
		log.db_set("error", f"Skipped: {doctype} {name} is cancelled. No fields overridden.")
		return

	local_doc = frappe.get_doc(doctype, name)
	sync_fields = get_sync_fields_for_doctype(doctype)

	# Submitted doc — only Allow on Submit fields that are ALSO specified in Sync Settings
	# If no sync_fields configured in Sync Settings — complete skip, nothing will be written
	if local_docstatus == 1:
		if not sync_fields:
			log.db_set("status", "Skipped")
			log.db_set("error", f"Skipped: {doctype} {name} is submitted. No sync fields configured in Sync Settings.")
			return

		meta = frappe.get_meta(doctype)
		allow_on_submit_fields = {df.fieldname for df in meta.fields if df.allow_on_submit}

		updated_fields = []
		for key, value in doc_data.items():
			if key in ("name", "doctype", "docstatus") or isinstance(value, list):
				continue
			# Must be Allow on Submit
			if key not in allow_on_submit_fields:
				continue
			# Must be specified in Sync Settings sync_fields
			if key not in sync_fields:
				continue
			local_doc.set(key, value)
			updated_fields.append(key)

		if updated_fields:
			local_doc.db_update()
			log.db_set("status", "Success")
			log.db_set("error", f"Submitted doc: updated only allow-on-submit fields: {', '.join(updated_fields)}")
		else:
			log.db_set("status", "Skipped")
			log.db_set("error", f"Skipped: {doctype} {name} is submitted. No matching allow-on-submit fields in sync settings.")
		return

	# Draft doc — override ALL fields
	conflict_strategy = get_conflict_strategy(doctype)
	local_modified = frappe.db.get_value(doctype, name, "modified")

	if conflict_strategy == "Last Write Wins":
		if str(modified_timestamp) < str(local_modified):
			log.db_set("status", "Skipped")
			return
	elif conflict_strategy == "Skip":
		log.db_set("status", "Skipped")
		return

	for key, value in doc_data.items():
		if key in ("name", "doctype") or isinstance(value, list):
			continue
		if sync_fields and key not in sync_fields:
			continue
		local_doc.set(key, value)
	local_doc.db_update()

	if not sync_fields:
		_sync_child_tables(doctype, name, doc_data)

	log.db_set("status", "Success")


def _handle_submit(doc_data, log):
	"""Handle an incoming submit event.
	- Doc exist nahi karta: insert karega
	- Doc exist karta hai (draft): submit karega (docstatus 0→1) + all fields update
	- Doc exist karta hai (submitted): ONLY Allow on Submit + Sync Settings fields
	- Doc exist karta hai (cancelled): SKIP
	"""
	doctype = doc_data.get("doctype")
	name = doc_data.get("name")

	if not frappe.db.exists(doctype, name):
		_handle_insert(doc_data, log)
		return

	current_docstatus = frappe.db.get_value(doctype, name, "docstatus")

	# Cancelled — SKIP
	if current_docstatus == 2:
		log.db_set("status", "Skipped")
		log.db_set("error", f"Skipped: {doctype} {name} is cancelled.")
		return

	local_doc = frappe.get_doc(doctype, name)
	sync_fields = get_sync_fields_for_doctype(doctype)

	# Already submitted — only Allow on Submit fields that are ALSO specified in Sync Settings
	# If no sync_fields configured — complete skip
	if current_docstatus == 1:
		if not sync_fields:
			log.db_set("status", "Skipped")
			log.db_set("error", f"Skipped: {doctype} {name} is submitted. No sync fields configured in Sync Settings.")
			return

		meta = frappe.get_meta(doctype)
		allow_on_submit_fields = {df.fieldname for df in meta.fields if df.allow_on_submit}

		updated_fields = []
		for key, value in doc_data.items():
			if key in ("name", "doctype", "docstatus") or isinstance(value, list):
				continue
			if key not in allow_on_submit_fields:
				continue
			if key not in sync_fields:
				continue
			local_doc.set(key, value)
			updated_fields.append(key)

		if updated_fields:
			local_doc.db_update()
			log.db_set("status", "Success")
			log.db_set("error", f"Submitted doc: updated only allow-on-submit fields: {', '.join(updated_fields)}")
		else:
			log.db_set("status", "Skipped")
			log.db_set("error", f"Skipped: {doctype} {name} is submitted. No matching allow-on-submit fields in sync settings.")
		return

	# Draft doc — submit it (docstatus 0→1) + update all fields
	for key, value in doc_data.items():
		if key in ("name", "doctype", "docstatus") or isinstance(value, list):
			continue
		if sync_fields and key not in sync_fields:
			continue
		local_doc.set(key, value)

	local_doc.docstatus = 1
	local_doc.db_update()

	if not sync_fields:
		_sync_child_tables(doctype, name, doc_data)

	log.db_set("status", "Success")


def _handle_cancel(doc_data, log):
	"""Handle an incoming cancel event.
	- Sync se cancel nahi hoga — manually karo
	- Submitted doc sync se cancel nahi hona chahiye
	"""
	doctype = doc_data.get("doctype")
	name = doc_data.get("name")

	# Always SKIP — cancel must be done manually
	log.db_set("status", "Skipped")
	log.db_set("error", f"Skipped: Cannot cancel {doctype} {name} via sync. Cancel must be done manually.")


def _sync_child_tables(parent_doctype, parent_name, doc_data):
	"""Sync child table rows via direct DB operations, bypassing all controller hooks."""
	for df in frappe.get_meta(parent_doctype).get_table_fields():
		if df.fieldname not in doc_data:
			continue

		rows = doc_data.get(df.fieldname) or []
		child_doctype = df.options

		existing = {
			r.name
			for r in frappe.get_all(
				child_doctype,
				filters={"parent": parent_name, "parentfield": df.fieldname},
				fields=["name"],
			)
		}
		incoming = {r.get("name") for r in rows if r.get("name")}

		# Delete rows removed on the source
		for row_name in existing - incoming:
			frappe.db.delete(child_doctype, {"name": row_name})

		# Upsert incoming rows
		for idx, row_data in enumerate(rows, start=1):
			if hasattr(row_data, "as_dict"):
				row_data = row_data.as_dict()

			row_name = row_data.get("name")
			# Skip nested child-of-child lists
			clean = {k: v for k, v in row_data.items() if not isinstance(v, list)}
			clean.update({
				"parent": parent_name,
				"parenttype": parent_doctype,
				"parentfield": df.fieldname,
				"idx": idx,
			})

			if row_name and row_name in existing:
				child_doc = frappe.get_doc(child_doctype, row_name)
				child_doc.update(clean)
				child_doc.db_update()
			else:
				child_doc = frappe.new_doc(child_doctype)
				child_doc.update(clean)
				if row_name:
					child_doc.name = row_name
					child_doc.flags.name_set = True
				child_doc.db_insert()


def _handle_delete(doctype, name, log):
	"""Handle an incoming delete event using raw SQL to bypass all ERPNext hooks."""
	if not frappe.db.exists(doctype, name):
		log.db_set("status", "Skipped")
		return

	# Skip if doc is submitted/cancelled
	docstatus = frappe.db.get_value(doctype, name, "docstatus")
	if docstatus in (1, 2):
		log.db_set("status", "Skipped")
		log.db_set("error", f"Cannot delete submitted/cancelled {doctype} {name} (docstatus={docstatus})")
		return

	# Delete child rows first (raw SQL to avoid any hook/validation)
	for df in frappe.get_meta(doctype).get_table_fields():
		frappe.db.sql(f"DELETE FROM tab{df.options} WHERE parent = %s", name)

	# Delete the parent document
	frappe.db.sql(f"DELETE FROM tab{doctype} WHERE name = %s", name)
	frappe.clear_document_cache(doctype, name)

	log.db_set("status", "Success")


def _create_sync_log(doctype_name, document_name, event, direction, origin_site_id,
	modified_timestamp, status="Queued", error=None):
	"""Create a Sync Log entry."""
	log = frappe.get_doc({
		"doctype": "Sync Log",
		"doctype_name": doctype_name,
		"document_name": document_name,
		"event": event,
		"direction": direction,
		"status": status,
		"origin_site_id": origin_site_id,
		"modified_timestamp": modified_timestamp,
		"error": error,
	})
	log.flags.ignore_permissions = True
	log.insert()
	return log


def _resolve_dependencies(dependencies, origin_site_id):
	"""Resolve Link field dependencies before inserting/updating a document."""
	if not dependencies:
		return

	resolving = frappe.flags.get("_sync_resolving_deps") or set()

	for dep in dependencies:
		key = (dep["doctype"], dep["name"])
		if key in resolving:
			continue

		if frappe.db.exists(dep["doctype"], dep["name"]):
			continue

		# Check if this doctype is in our synced list
		settings = get_sync_settings()
		synced_doctypes = [row.doctype_name for row in settings.synced_doctypes]

		if dep["doctype"] not in synced_doctypes:
			frappe.log_error(
				title="Sync Dependency Missing",
				message=f"Missing dependency: {dep['doctype']} {dep['name']} (not in synced doctypes)",
			)
			continue

		# Mark as resolving to prevent circular deps
		resolving.add(key)
		frappe.flags._sync_resolving_deps = resolving

		# Try to fetch from the remote that sent this sync
		# For now, log a warning - the retry mechanism will handle this
		frappe.log_error(
			title="Sync Dependency Missing",
			message=f"Missing dependency: {dep['doctype']} {dep['name']}. Will be resolved on retry.",
		)

		resolving.discard(key)