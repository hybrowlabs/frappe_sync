import frappe
from frappe import _

from frappe_sync.frappe_sync.utils import (
	get_enabled_connections,
	get_event_type,
	get_sync_settings,
	is_sync_enabled_for_doctype,
	prepare_doc_payload,
)

# DocTypes that should never be synced (internal/system doctypes)
EXCLUDED_DOCTYPES = {
	"Sync Settings",
	"Sync Connection",
	"Sync DocType",
	"Sync Log",
	"Error Log",
	"Scheduled Job Log",
	"Activity Log",
	"Access Log",
	"Route History",
	"Version",
	"Comment",
	"Communication",
}


def on_document_change(doc, method):
	"""Hook called on after_insert, on_update, on_trash for all doctypes.

	Determines if this doctype is configured for sync, checks the sync-loop
	flag, and enqueues the sync job.
	"""
	# Layer 1: Prevent sync loops
	if getattr(frappe.flags, "in_frappe_sync", False):
		return

	# Skip excluded system doctypes
	if doc.doctype in EXCLUDED_DOCTYPES:
		return

	# on_update fires as a side-effect of every save, including submit and cancel.
	# Submitted/cancelled docs have dedicated hooks (on_submit, on_cancel,
	# on_update_after_submit), so skip on_update for them to avoid double-logging.
	if method == "on_update" and (doc.docstatus or 0) != 0:
		return

	# after_insert is immediately followed by on_update within the same insert() call.
	# Use doc.flags (tied to the doc instance) to skip the redundant on_update.
	if method == "after_insert":
		doc.flags.frappe_sync_inserted = True
	elif method == "on_update" and doc.flags.get("frappe_sync_inserted"):
		return

	# on_submit is immediately followed by on_update_after_submit within submit().
	# Skip the redundant on_update_after_submit so only on_submit creates a log.
	if method == "on_submit":
		doc.flags.frappe_sync_submitted = True
	elif method == "on_update_after_submit" and doc.flags.get("frappe_sync_submitted"):
		return

	# Skip if sync is not enabled for this doctype + event
	if not is_sync_enabled_for_doctype(doc.doctype, method):
		return

	settings = get_sync_settings()
	origin_site_id = settings.site_id

	event = get_event_type(method)

	# Always log deletions so pull clients can discover them via get_deletions_since,
	# regardless of whether any Push/Pull connections are configured on this server.
	if event == "Delete":
		_log_deletion_for_pull(doc, origin_site_id)

	connections = get_enabled_connections()

	if not connections:
		return

	payload = prepare_doc_payload(doc, event)

	for connection in connections:
		# Layer 2: Don't send back to the originator (for forwarded syncs)
		if connection.remote_site_id == origin_site_id:
			continue

		sync_mode = frappe.db.get_value("Sync Connection", connection.name, "sync_mode") or "Push"

		if sync_mode == "Pull":
			# Pull clients poll us for changes; deletion already logged above.
			continue

		frappe.enqueue(
			"frappe_sync.frappe_sync.sync_engine.push_to_remote",
			queue="short",
			doc_data=payload,
			connection_name=connection.name,
			sync_event=event,
			origin_site_id=origin_site_id,
			modified_timestamp=str(doc.modified),
		)


def push_to_remote(doc_data, connection_name, sync_event, origin_site_id, modified_timestamp):
	"""Background job that pushes a document change to a remote Frappe instance."""
	import requests as _requests

	log = frappe.get_doc({
		"doctype": "Sync Log",
		"doctype_name": doc_data.get("doctype"),
		"document_name": doc_data.get("name"),
		"sync_connection": connection_name,
		"event": sync_event,
		"direction": "Outgoing",
		"request_payload": frappe.as_json(doc_data),
		"origin_site_id": origin_site_id,
		"modified_timestamp": modified_timestamp,
	})

	try:
		connection = frappe.get_doc("Sync Connection", connection_name)
		api_secret = connection.get_password("api_secret")
		base_url = connection.remote_url.rstrip("/")

		headers = {
			"Authorization": f"token {connection.api_key}:{api_secret}",
			"Content-Type": "application/x-www-form-urlencoded",
			"Accept": "application/json",
		}

		# For multi-tenant setups, set the Host header to route to the correct site
		if connection.site_name:
			headers["Host"] = connection.site_name

		resp = _requests.post(
			f"{base_url}/api/method/frappe_sync.frappe_sync.api.receive_sync",
			headers=headers,
			data={
				"doc_data": frappe.as_json(doc_data),
				"event": sync_event,
				"origin_site_id": origin_site_id,
				"modified_timestamp": modified_timestamp,
			},
			timeout=30,
		)

		if resp.status_code != 200:
			raise Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")

		response = resp.json().get("message") or {}

		log.status = "Success"
		connection.db_set("last_sync_at", frappe.utils.now_datetime())
		connection.db_set("status", "Active")

	except Exception:
		log.status = "Failed"
		log.error = frappe.get_traceback()
		log.retry_count = 0
		log.next_retry_at = _calculate_next_retry(0)

		try:
			frappe.get_doc("Sync Connection", connection_name).db_set("status", "Error")
		except Exception:
			pass

		log.flags.ignore_permissions = True
		log.insert()
		frappe.db.commit()
		raise

	log.flags.ignore_permissions = True
	log.insert()
	frappe.db.commit()


def _log_deletion_for_pull(doc, origin_site_id):
	"""Record a deletion in Sync Log so pull clients can discover it via get_deletions_since."""
	log = frappe.get_doc({
		"doctype": "Sync Log",
		"doctype_name": doc.doctype,
		"document_name": doc.name,
		"event": "Delete",
		"direction": "Outgoing",
		"status": "Success",
		"origin_site_id": origin_site_id,
	})
	log.flags.ignore_permissions = True
	log.insert()
	frappe.db.commit()


def pull_from_remotes():
	"""Scheduler entry point: enqueue a pull job for every Pull-mode connection."""
	settings = get_sync_settings()
	if not settings.enabled:
		return

	connections = frappe.get_all(
		"Sync Connection",
		filters={"enabled": 1, "sync_mode": ("in", ("Pull", "Push & Pull"))},
		fields=["name"],
	)
	for conn in connections:
		frappe.enqueue(
			"frappe_sync.frappe_sync.sync_engine.pull_from_remote",
			connection_name=conn.name,
			queue="short",
		)


def pull_from_remote(connection_name):
	"""Background job: poll a remote server for changes and apply them locally."""
	import requests as _requests

	from frappe_sync.frappe_sync.api import _handle_cancel, _handle_delete, _handle_insert, _handle_update, _handle_submit, _create_sync_log, _resolve_dependencies

	connection = frappe.get_doc("Sync Connection", connection_name)
	api_secret = connection.get_password("api_secret")
	base_url = connection.remote_url.rstrip("/")
	since = connection.last_pull_at or "2000-01-01 00:00:00"

	headers = {
		"Authorization": f"token {connection.api_key}:{api_secret}",
		"Accept": "application/json",
	}
	if connection.site_name:
		headers["Host"] = connection.site_name

	try:
		resp = _requests.get(
			f"{base_url}/api/method/frappe_sync.frappe_sync.api.get_changes_since",
			headers=headers,
			params={"since_timestamp": str(since)},
			timeout=30,
		)

		if resp.status_code != 200:
			raise Exception(f"HTTP {resp.status_code}: {resp.text[:500]}")

		changes = resp.json().get("message") or []

		frappe.flags.in_frappe_sync = True
		origin_site_id = connection.remote_site_id
		last_timestamp = None

		for item in changes:
			doc_data = item.get("doc_data") or {}
			if isinstance(doc_data, str):
				doc_data = frappe.parse_json(doc_data)

			modified_timestamp = item.get("modified_timestamp")
			dependencies = doc_data.pop("_dependencies", [])
			doctype = doc_data.get("doctype")
			name = doc_data.get("name")
			docstatus = doc_data.get("docstatus", 0)

			# Skip docs already at the same version — avoids logs for unchanged docs
			# when last_pull_at loses sub-second precision on storage and the same
			# doc appears again in get_changes_since on the next scheduler tick.
			if modified_timestamp and frappe.db.exists(doctype, name):
				local_modified = frappe.db.get_value(doctype, name, "modified")
				if str(modified_timestamp) <= str(local_modified):
					if modified_timestamp:
						last_timestamp = modified_timestamp
					continue

			# Determine the right event from current docstatus
			if docstatus == 1:
				event = "Submit"
			elif docstatus == 2:
				event = "Cancel"
			else:
				event = "Update"

			log = _create_sync_log(doctype, name, event, "Incoming", origin_site_id, modified_timestamp)
			_resolve_dependencies(dependencies, origin_site_id)

			try:
				if event == "Submit":
					_handle_submit(doc_data, log)
				elif event == "Cancel":
					_handle_cancel(doc_data, log)
				else:
					_handle_update(doc_data, modified_timestamp, log)
				frappe.db.commit()
			except Exception:
				frappe.db.rollback()
				log.db_set("status", "Failed")
				log.db_set("error", frappe.get_traceback())
				frappe.db.commit()

			# Always update last_timestamp — even for Skipped docs
			# This prevents same docs from being pulled again and again
			if modified_timestamp:
				last_timestamp = modified_timestamp

		if last_timestamp:
			connection.db_set("last_pull_at", last_timestamp)

		# Also pull deletions — deleted docs don't appear in get_changes_since
		del_resp = _requests.get(
			f"{base_url}/api/method/frappe_sync.frappe_sync.api.get_deletions_since",
			headers=headers,
			params={"since_timestamp": str(since)},
			timeout=30,
		)
		if del_resp.status_code == 200:
			last_deletion_timestamp = None
			for item in del_resp.json().get("message") or []:
				doctype = item.get("doctype_name")
				name = item.get("document_name")
				creation = item.get("creation")
				log = _create_sync_log(doctype, name, "Delete", "Incoming", origin_site_id, None)
				try:
					_handle_delete(doctype, name, log)
					frappe.db.commit()
				except Exception:
					frappe.db.rollback()
					log.db_set("status", "Failed")
					log.db_set("error", frappe.get_traceback())
					frappe.db.commit()

				if creation:
					last_deletion_timestamp = creation

			# Advance last_pull_at past processed deletions so the same deletion
			# log is not returned again on the next scheduler tick.
			if last_deletion_timestamp:
				if not last_timestamp or str(last_deletion_timestamp) > str(last_timestamp):
					connection.db_set("last_pull_at", last_deletion_timestamp)

		connection.db_set("status", "Active")

	except Exception:
		try:
			connection.db_set("status", "Error")
		except Exception:
			pass
		frappe.log_error(title="Pull Sync Failed", message=frappe.get_traceback())

	finally:
		frappe.flags.in_frappe_sync = False


def _calculate_next_retry(retry_count):
	print("hhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
	"""Exponential backoff: 1min, 5min, 15min, 1hr, 6hr."""
	delays = [60, 300, 900, 3600, 21600]
	delay = delays[min(retry_count, len(delays) - 1)]
	return frappe.utils.add_to_date(frappe.utils.now_datetime(), seconds=delay)