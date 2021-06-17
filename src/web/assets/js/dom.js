function empty(node) {
    while (node.firstChild) {
        node.removeChild(node.firstChild);
    }
}

function node(tag, props, children) {
    const tg = document.createElement(tag);
    Object.keys(props || {}).forEach(k => tg.setAttribute(k, props[k]));
    if (children === undefined) {
        return tg;
    }
    if (Array.isArray(children)) {
        children.forEach(child => tg.append(child));
    } else {
        tg.append(children);
    }
    return tg;
}

export { empty, node };