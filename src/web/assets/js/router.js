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
        // TODO(PR): move this to the "nav-panel" component we'll build instead of <nav>
        // this component will also route the top level links and eliminate the need for all this?
        const currentClass = "current";
        for (let link of document.querySelectorAll("nav#panel ul li a")) {
            if (link.classList.contains(currentClass)) {
                link.classList.remove(currentClass);
            }
            if (link.getAttribute("href") === window.location.pathname) {
                link.classList.add(currentClass);
            }
        }
    }
}

new Router();
