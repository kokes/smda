// TODO(PR): perhaps eliminate the router by implementing all the routing logic
// in individual components
// Or perhaps migrate this to main.js
class Router {
    constructor() {
        window.onload = e => this.route();
        window.onpopstate = e => {
            this.route()
        }
    }
    route() {
        const currentClass = "current";
        for (let link of document.querySelectorAll("nav#panel ul li a")) {
            if (link.classList.contains(currentClass)) {
                link.classList.remove(currentClass);
            }
            if (link.getAttribute("href") === window.location.pathname) {
                link.classList.add(currentClass);
            }
        }

        const path = window.location.pathname.slice(1).split("/");
        let route = path[0];
        if (route === "") {
            route = "root";
        }
        document.querySelectorAll("main > div").forEach(x => x.style.display = "none");
        // ARCH: pass this down to submitQuery/setupDatasets?
        const target = document.getElementById(`route-${route}`);
        if (target !== null) {
            target.style.display = "block";
        }
        if (route === "query") {
            const params = new URLSearchParams(window.location.search);
            const query = params.get("sql");
            // TODO(PR): is there a better way to tie components together?
            document.querySelector("query-window").updateQuery(query);
        }
    }
}

new Router();
