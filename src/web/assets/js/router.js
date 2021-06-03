class Router {
    constructor(routes) {
        this.routes = routes;

        window.onload = e => this.route();
        window.onpopstate = e => {
            this.route()
        }

        document.addEventListener("click", e => {
            // ARCH: input[type=submit]?
            if (!(e.target.nodeName === "BUTTON" || e.target.nodeName === "A")) {
                return;
            }
            e.preventDefault();

            switch (e.target.nodeName) {
                case "A":
                    const link = e.target.getAttribute("href");
                    history.pushState({}, "", link);
                    break;
                case "BUTTON":
                    const url = new URL(window.location);
                    url.search = '';
                    const qform = e.target.closest("form");
                    if (qform.method !== "get") {
                        throw new Error("cannot submit POST forms yet");
                    }
                    for (let entry of (new FormData(qform)).entries()) {
                        url.searchParams.set(entry[0], entry[1]);
                    }
                    history.pushState({}, "", url);
                    break;
                default:
                    console.error(`unregistered click on ${e.target.nodeName}`);
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

        const qform = document.forms["query"];
        if (qform !== undefined) {
            const params = new URLSearchParams(window.location.search);
            for (let inp of qform.querySelectorAll("input, select")) {
                const fieldName = inp.getAttribute("name");
                inp.value = params.get(fieldName);
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

        if (!this.routes.hasOwnProperty(route)) {
            throw new Error(`route ${route} not implemented`);
        }
        this.routes[route]();
    }
}

export { Router };
