class Router {
    constructor(routes) {
        this.routes = routes;

        window.onload = e => this.route();
        window.onpopstate = e => {
            this.route()
        }

        document.addEventListener("click", e => {
            const target = e.composedPath()[0]; // TODO(PR): added because of WebComponents (remove once we chain them)
            // ARCH: input[type=submit]?
            if (!(target.nodeName === "BUTTON" || target.nodeName === "A")) {
                return;
            }
            e.preventDefault();

            switch (target.nodeName) {
                case "A":
                    const link = target.getAttribute("href");
                    history.pushState({}, "", link);
                    break;
                case "BUTTON":
                    const url = new URL(window.location);
                    url.search = '';
                    const qform = target.closest("form");
                    if (qform.method !== "get") {
                        throw new Error("cannot submit POST forms yet");
                    }
                    for (let entry of (new FormData(qform)).entries()) {
                        url.searchParams.set(entry[0], entry[1]);
                    }
                    history.pushState({}, "", url);
                    break;
                default:
                    console.error(`unregistered click on ${target.nodeName}`);
            }

            this.route()
        });
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
            document.querySelector("query-window").shadowRoot.querySelector("textarea").innerHTML = query;
        }

        if (this.routes.hasOwnProperty(route)) {
            this.routes[route]();
        }
    }
}

export { Router };
