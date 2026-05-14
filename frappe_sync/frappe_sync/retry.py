import frappe

MAX_RETRIES = 5


def process_failed_syncs():
	"""Retry failed sync logs with exponential backoff.

	Called by scheduler every 5 minutes.
	"""
	failed_logs = frappe.get_all(
		"Sync Log",
		filters={
			"status": "Failed",
			"direction": "Outgoing",
			"retry_count": ["<", MAX_RETRIES],
			"next_retry_at": ["<=", frappe.utils.now_datetime()],
		},
		fields=[
			"name",
			"request_payload",
			"sync_connection",
			"event",
			"origin_site_id",
			"modified_timestamp",
			"retry_count",
		],
		limit=50,
	)

	for log_data in failed_logs:
		_retry_sync(log_data)


def _retry_sync(log_data):
	"""Retry a single failed sync.

	Updates the original log's retry_count to MAX_RETRIES after the attempt so
	process_failed_syncs never picks it up again (prevents infinite retry loop).
	On failure a new Sync Log is created to preserve the audit trail.
	"""
	from frappe_sync.frappe_sync.sync_engine import push_to_remote

	retry_count = log_data.retry_count + 1
	doc_data = frappe.parse_json(log_data.request_payload)

	try:
		push_to_remote(
			doc_data=doc_data,
			connection_name=log_data.sync_connection,
			sync_event=log_data.event,
			origin_site_id=log_data.origin_site_id,
			modified_timestamp=log_data.modified_timestamp,
		)

	except Exception:
		frappe.get_doc({
			"doctype": "Sync Log",
			"doctype_name": doc_data.get("doctype", ""),
			"document_name": doc_data.get("name", ""),
			"event": log_data.event,
			"direction": "Outgoing",
			"status": "Failed",
			"sync_connection": log_data.sync_connection,
			"origin_site_id": log_data.origin_site_id,
			"modified_timestamp": log_data.modified_timestamp,
			"retry_count": retry_count,
			"next_retry_at": _calculate_next_retry(retry_count) if retry_count < MAX_RETRIES else None,
			"error": f"Retry #{retry_count} of {log_data.name}\n{frappe.get_traceback()}",
			"request_payload": log_data.request_payload,
		}).insert(ignore_permissions=True)

	# Exclude original log from future retries regardless of success/failure.
	# Without this, process_failed_syncs keeps re-picking the same Failed log forever.
	frappe.db.set_value("Sync Log", log_data.name, "retry_count", MAX_RETRIES)
	frappe.db.commit()


def _calculate_next_retry(retry_count):
	"""Exponential backoff: 1min, 5min, 15min, 1hr, 6hr."""
	delays = [60, 300, 900, 3600, 21600]
	delay = delays[min(retry_count, len(delays) - 1)]
	return frappe.utils.add_to_date(frappe.utils.now_datetime(), seconds=delay)