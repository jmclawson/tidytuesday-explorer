import * as duckdb from "https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.0/+esm";
const metaHeader = document.getElementById("meta");
const searchColumn = document.getElementById("pickColumn");
const searchInput = document.getElementById("searchVariable");
const output = document.getElementById("tableHere");
const breadcrumb_outer = document.getElementById("pathHere");
const breadcrumb = document.getElementById("innerPath");
const moreOutput = document.getElementById("moreHere");
const limitInput = document.getElementById("numResults");

let dlContext;
let dlValue;

let conn;

let meta_vars;
let meta_files;
let meta_sets;

async function setup() {
    const loading = document.getElementById("loading");

    const bundle = {
        mainModule: new URL("./duckdb/duckdb-eh.wasm", window.location.href).toString(),
        mainWorker: new URL("./duckdb/duckdb-browser-eh.worker.js", window.location.href).toString(),
    };

    const worker = new Worker(bundle.mainWorker);
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);

    await db.instantiate(bundle.mainModule);
    conn = await db.connect();

    const parquetUrl = new URL("./data/tt_columns.parquet", window.location.href).toString();

    await conn.query(`
        CREATE VIEW variables AS
        SELECT * FROM parquet_scan('${parquetUrl}')
    `);

    const meta_vars_result = await conn.query(`
        SELECT COUNT(*) as n_rows
        FROM variables
    `);
    const meta_files_result = await conn.query(`
        SELECT COUNT(DISTINCT source_file) AS n_files
        FROM variables;
    `);
    const meta_sets_result = await conn.query(`
        SELECT COUNT(DISTINCT dataset_id) AS n_sets
        FROM variables;
    `);

    const meta_sets = meta_sets_result.toArray()[0].n_sets;
    const meta_files = meta_files_result.toArray()[0].n_files;
    const meta_vars = meta_vars_result.toArray()[0].n_rows.toLocaleString();

    metaHeader.innerHTML = `${meta_sets} datasets &bull; ${meta_files} files &bull; ${meta_vars} variables`;
    loading.style.display = "none";
}

async function runQuery(searchCol, searchTerm, searchOrder, limitNum) {
    const result = await conn.query(`
        SELECT dataset_id, dataset_title, variable, variable_description
        FROM variables
        WHERE ${searchCol} LIKE '%${searchTerm}%'
        ORDER BY dataset_id ${searchOrder}
        LIMIT ${limitNum}
    `);
    
    const rows = result.toArray();
    
    if (rows.length === 0) {
        output.innerText = "No results";
        return;
    }
    
    output.innerHTML = "";  // clear previous content
    breadcrumb.innerHTML = "";  // clear previous content
    breadcrumb_outer.classList.remove("has-content");
    moreOutput.innerHTML = "";  // clear previous content
    moreOutput.classList.add("empty")
    const subHeader = document.createElement("span");
    subHeader.textContent = `Click a row to explore by dataset.`;
    subHeader.classList.add("subheader")
    output.appendChild(subHeader)
    output.appendChild(renderTable(rows, "startingpoint"));
}

function renderTable(rows, drillview) {
    let clear_breadcrumb;
    if (drillview === "startingpoint") {
        drillview = "dataset_id";
        clear_breadcrumb = true;
    } else {
        clear_breadcrumb = false;
    }
    let nextView;
    if (drillview === "dataset_id") {
        nextView = "variable";
    } else if (drillview === "variable") {
        nextView = "dataset_id";
    }
    const table = document.createElement("table");
    
    const headers = Object.keys(rows[0]);
    
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    
    headers.forEach(h => {
        const th = document.createElement("th");
        th.textContent = h;
        headerRow.appendChild(th);
    });
    
    thead.appendChild(headerRow);
    table.appendChild(thead);
    
    const tbody = document.createElement("tbody");
    const orderDir = document.querySelector('input[name="order"]:checked').value;
    
    rows.forEach(row => {
        const tr = document.createElement("tr");
        tr.addEventListener("click", async () => {
            if (clear_breadcrumb === true) {
                breadcrumb.innerHTML = "";
                breadcrumb_outer.classList.remove("has-content");
            }
            drillDown(row, drillview, nextView, orderDir);
        });
    
        headers.forEach(h => {
            const td = document.createElement("td");
            td.textContent = row[h];
            tr.appendChild(td);
        });
    
        tbody.appendChild(tr);
    });


    table.appendChild(tbody);
    return table;
}

function renderBreadcrumb(row, drillview, nextView) {
    const crumb = document.createElement("span");
    crumb.textContent = row[drillview];
    crumb.classList.add(drillview);

    crumb.addEventListener("click", () => {
        let next = crumb.nextSibling;
        const orderDir = document.querySelector('input[name="order"]:checked').value;
        while (next) {
            const toRemove = next;
            next = next.nextSibling;
            toRemove.remove();
        }
        breadcrumb.lastElementChild.remove();
        breadcrumb.lastElementChild.remove();
        drillDown(row, drillview, nextView, orderDir);
        })
    return crumb
}

async function drillDown(row, drillview, nextView, searchOrder) {
    let result;
    dlContext = drillview;
    dlValue = row[drillview]
    if (drillview === "variable") {
        result = await conn.query(`
            SELECT dataset_id, dataset_title, source_file, variable_description
            FROM variables
            WHERE ${drillview} = '${row[drillview]}'
            ORDER BY dataset_id ${searchOrder}
            LIMIT 20
        `);
    } else {
        result = await conn.query(`
            SELECT source_file, variable, variable_description
            FROM variables
            WHERE ${drillview} = '${row[drillview]}'
            ORDER BY dataset_id ${searchOrder}
            LIMIT 20
        `);
    }

    const detailRows = result.toArray();

    // update breadcrumb
    const breadcrumbArrow = document.createElement("b");
    breadcrumbArrow.innerHTML = "&#8227;";
    breadcrumb_outer.classList.add("has-content");
    breadcrumb.appendChild(breadcrumbArrow);
    breadcrumb.appendChild(renderBreadcrumb(row, drillview, nextView));

    // update table
    moreOutput.classList.remove("empty");
    moreOutput.classList.add("notempty");
    moreOutput.innerHTML = "";
    const moreHeader = document.createElement("span");
    if (drillview === "dataset_id") {
        moreHeader.innerHTML = `Data dictionary for week ${row[drillview]}: <b>${row["dataset_title"]}</b>`;
    } else if (drillview === "variable") {
        moreHeader.innerHTML = `Datasets containing variable "<b>${row[drillview]}</b>"`;
    }
    moreHeader.classList.add("more_header");
    moreOutput.appendChild(moreHeader);

    const n_rows_result = await conn.query(`
        SELECT COUNT(*) as n_rows
        FROM variables
        WHERE ${dlContext} = '${dlValue}'
    `)
    
    const n_rows = n_rows_result.toArray()[0].n_rows;

    let result_label;
    if (nextView == "variable") {
        result_label = "variables";
    } else {
        result_label = "datasets";
    }

    if (n_rows < 2) {
        result_label = result_label.slice(0, -1);
    }

    let n_results;
    if (n_rows > 20) {
        n_results = `${n_rows} ${result_label}, showing the first 20`;
    } else {
        n_results = `${n_rows} ${result_label}`;
    }

    const moreSubheader = document.createElement("span");
    // moreSubheader.textContent = `Limited to 20. Click a row to explore by ${nextView}.`;
    const url_repo = "https://tidytues.day";

    let dl_filename
    let dl_descriptor
    if (dlContext === "dataset_id") {
        dl_filename = "tt_dict_" + dlValue + ".csv"
        dl_descriptor = "dictionary"
    } else {
        dl_filename = "tt_var_" + dlValue + ".csv"
        dl_descriptor = "results"
    }

    const download_link = document.createElement("span");
    download_link.classList.add("download");
    const dl_link = document.createElement("a");
    dl_link.href = "#";
    dl_link.textContent = "Download the " + dl_descriptor;

    download_link.appendChild(dl_link);

    moreSubheader.append(`${n_results}. `);
    moreSubheader.append(dl_link);
    moreSubheader.append(", ");
    if (drillview === "dataset_id") {
        // const url_data = "tree/main/data";
        const url_year = row["dataset_id"].slice(0, 4);
        const url_dir = row["dataset_id"];
        
        // const url = [url_repo, url_data, url_year, url_dir].join("/");
        const url = [url_repo, url_year, url_dir].join("/");

        const repo = document.createElement("a");
        repo.href = url;
        repo.target = "_blank";
        repo.textContent = "this dataset online";

        moreSubheader.append("find ", repo, ", or click a row to find datasets with that variable.");
    } else if (drillview === "variable") {
        const repo = document.createElement("a");
        repo.href = url_repo;
        repo.target = "_blank";
        repo.textContent = "TidyTuesday project";

        moreSubheader.append("visit the ", repo, ", or click a row to explore that dataset.");
    }
    moreSubheader.classList.add("more_subheader");
    moreOutput.appendChild(moreSubheader);
    
    moreOutput.appendChild(renderTable(detailRows, nextView));

    dl_link.addEventListener("click", () => {
        downloadQueryAsCSV(`
            SELECT * FROM variables
            WHERE ${dlContext} = '${dlValue}'
            ORDER BY dataset_id DESC
        `, dl_filename);
    });

    moreOutput.appendChild(download_link);
}

async function downloadQueryAsCSV(query, filename = "results.csv") {
    const result = await conn.query(query);
    const rows = result.toArray();
            
    if (rows.length === 0) {
        alert("No data to download.");
        return;
    }

    // Convert to CSV
    const headers = Object.keys(rows[0]);
    const csv = [
        headers.join(","),
        ...rows.map(row =>
            headers.map(h => JSON.stringify(row[h] ?? "")).join(",")
        )
    ].join("\n");
            
    // Create downloadable blob
    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
            
    // Trigger download
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
            
    URL.revokeObjectURL(url);
}

searchColumn.addEventListener("input", () => {
    const limit = Number(limitInput.value) || 10;
    const orderDir = document.querySelector('input[name="order"]:checked').value;
    runQuery(
        searchColumn.value,
        searchInput.value,
        orderDir, 
        limit);
});

searchInput.addEventListener("input", () => {
    const limit = Number(limitInput.value) || 10;
    const orderDir = document.querySelector('input[name="order"]:checked').value;
    runQuery(
        searchColumn.value,
        searchInput.value,
        orderDir, 
        limit);
});

limitInput.addEventListener("change", () => {
    const limit = Number(limitInput.value) || 10;
    const orderDir = document.querySelector('input[name="order"]:checked').value;
    runQuery(
        searchColumn.value,
        searchInput.value,
        orderDir, 
        limit);
});

document.querySelectorAll('input[name="order"]').forEach(radio => {
    radio.addEventListener("change", () => {
        const limit = Number(limitInput.value) || 10;
        const orderDir = document.querySelector('input[name="order"]:checked').value;

        runQuery(
            searchColumn.value,
            searchInput.value,
            orderDir,
            limit
        );
    });
});

await setup();
await runQuery("variable_norm", "year", "DESC", 10);
