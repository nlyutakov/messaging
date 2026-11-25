const element = id => document.getElementById(id);
const names = ["Pending", "Replaying", "Replayed", "Archived", "Discarded"];
const labels = ["Pending", "Replay in progress or outcome unknown", "Replayed", "Archived", "Discarded"];
const pageSize = 50;
const dateFormat = new Intl.DateTimeFormat("en-GB", { dateStyle: "short", timeStyle: "short" });
let token = "";
let session = new AbortController();
let request = 0;
let cursors = [0];
let nextCursor;
let selected;

async function api(path, method = "GET") {
    const response = await fetch("/api/dead-letters" + path, {
        method,
        headers: { Authorization: "Bearer " + token },
        signal: session.signal,
        cache: "no-store"
    });
    if (!response.ok) {
        const messages = {
            401: "The token is invalid or has expired.",
            403: "You do not have permission for this action.",
            404: "This record is no longer available.",
            409: "The action was rejected. Check the record status, target queue, and replay limit.",
            503: "The replay outcome is unknown. Verify delivery before taking further action."
        };
        throw new Error(messages[response.status] ?? "The request failed.");
    }
    return response.json();
}

function node(tag, value) {
    const result = document.createElement(tag);
    result.textContent = value;
    return result;
}

function resetSession() {
    session.abort();
    session = new AbortController();
    request++;
    token = "";
    cursors = [0];
    selected = undefined;
    element("dashboard").hidden = true;
    element("disconnect").hidden = true;
    element("rows").replaceChildren();
    element("counts").replaceChildren();
    element("metadata").replaceChildren();
    element("payload").textContent = "";
    element("details").close();
}

async function load() {
    const current = ++request;
    const query = new URLSearchParams({ after: cursors.at(-1), limit: pageSize + 1 });
    if (element("filter").value) {
        query.set("status", element("filter").value);
    }
    element("previous").disabled = true;
    element("next").disabled = true;
    try {
        const [items, counts] = await Promise.all([api("/?" + query), api("/counts")]);
        if (current !== request) {
            return;
        }
        const page = items.slice(0, pageSize);
        nextCursor = items.length > pageSize ? page.at(-1).id : undefined;
        element("counts").replaceChildren(...names.map((name, index) => {
            const item = node("li", labels[index]);
            item.append(node("strong", counts[name] ?? 0));
            return item;
        }));
        element("rows").replaceChildren(...page.map(item => {
            const row = node("tr", "");
            const fields = [dateFormat.format(new Date(item.receivedAt)), item.messageType, item.originalQueue, item.failure, labels[item.status]];
            row.append(...fields.map(field => node("td", field ?? "—")));
            const cell = node("td", "");
            const button = node("button", "Details");
            button.type = "button";
            button.setAttribute("aria-label", `Details for record ${item.id}`);
            button.addEventListener("click", () => show(item));
            cell.append(button);
            row.append(cell);
            return row;
        }));
        element("empty").hidden = page.length > 0;
        element("previous").disabled = cursors.length === 1;
        element("next").disabled = nextCursor === undefined;
        element("dashboard").hidden = false;
        element("disconnect").hidden = false;
        element("notice").textContent = "";
    } catch (error) {
        if (current === request) {
            element("notice").textContent = error.message;
            element("previous").disabled = cursors.length === 1;
        }
    }
}

function metadata(values) {
    element("metadata").replaceChildren(...Object.entries(values)
        .flatMap(([label, value]) => [node("dt", label), node("dd", value ?? "—")]));
}

async function show(item) {
    selected = item.id;
    element("detail-title").textContent = `Message ${item.id}`;
    element("detail-notice").textContent = "Loading…";
    element("payload").hidden = true;
    element("payload").textContent = "";
    metadata({
        "Status": labels[item.status], "DLQ": item.deadLetterQueue, "Original queue": item.originalQueue,
        "MessageId": item.messageId, "Type": item.messageType, "Reason": item.failure, "Replay attempts": item.replayCount
    });
    for (const button of document.querySelectorAll("[data-action]")) {
        button.disabled = item.status !== 0 && !(item.status === 3 && button.dataset.action === "discard");
    }
    element("details").showModal();
    try {
        const detail = await api("/" + item.id);
        if (selected !== item.id || !element("details").open) {
            return;
        }
        for (const [name, value] of Object.entries({ "Content type": detail.contentType, "CorrelationId": detail.correlationId, ...detail.headers })) {
            element("metadata").append(node("dt", name), node("dd", value ?? "—"));
        }
        if (detail.payload !== null) {
            let payload = detail.payload;
            if (detail.contentType?.includes("json") || detail.contentType?.startsWith("text/")) {
                payload = new TextDecoder().decode(Uint8Array.from(atob(payload), character => character.charCodeAt(0)));
            }
            element("payload").textContent = payload;
            element("payload").hidden = false;
        }
        element("detail-notice").textContent = "";
    } catch (error) {
        if (selected === item.id && element("details").open) {
            element("detail-notice").textContent = error.message;
        }
    }
}

element("connect").addEventListener("submit", event => {
    event.preventDefault();
    resetSession();
    token = element("token").value.trim();
    element("token").value = "";
    if (token) {
        element("notice").textContent = "Connecting…";
        load();
    }
});
element("disconnect").addEventListener("click", () => {
    resetSession();
    element("notice").textContent = "Disconnected.";
});
element("refresh").addEventListener("click", load);
element("filter").addEventListener("change", () => { cursors = [0]; load(); });
element("previous").addEventListener("click", () => { cursors.pop(); load(); });
element("next").addEventListener("click", () => { cursors.push(nextCursor); load(); });
element("close").addEventListener("click", () => element("details").close());
element("details").addEventListener("close", () => {
    selected = undefined;
    element("payload").textContent = "";
});

const prompts = {
    replay: "This will send the message to its original queue.",
    archive: "This will mark the record as archived.",
    discard: "This will remove the payload and additional headers from the record."
};
for (const button of document.querySelectorAll("[data-action]")) {
    button.addEventListener("click", () => {
        const action = button.dataset.action;
        const id = selected;
        const confirmation = element("confirm");
        confirmation.returnValue = "";
        element("confirm-text").textContent = prompts[action];
        confirmation.addEventListener("close", async () => {
            if (confirmation.returnValue !== "apply" || id !== selected) {
                return;
            }
            const actions = [...document.querySelectorAll("[data-action]")];
            const previous = actions.map(actionButton => actionButton.disabled);
            actions.forEach(actionButton => { actionButton.disabled = true; });
            element("detail-notice").textContent = "Working…";
            try {
                await api(`/${id}/${action}`, "POST");
                element("details").close();
                await load();
                element("notice").textContent = "Action completed.";
            } catch (error) {
                if (selected === id) {
                    element("detail-notice").textContent = error.message;
                    actions.forEach((actionButton, index) => { actionButton.disabled = previous[index]; });
                }
            }
        }, { once: true });
        confirmation.showModal();
    });
}
