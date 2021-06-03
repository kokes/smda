// TODO: test (0, 999, 1001, trailing zeroes etc.)
function formatBytes(nbytes) {
    if (nbytes === 0) {
        return "0 B"
    }
    const units = ["B", "KB", "MB", "GB", "TB", "PB"]; // that should be enough :-)
    const scale = Math.floor(Math.log10(nbytes)/3);
    let fixed = (nbytes/Math.pow(10, 3*scale)).toFixed(2);
    if (fixed.endsWith(".00")) {
        fixed = fixed.slice(0, fixed.length - 3);
    }

    return `${fixed} ${units[scale]}`;
}

// TODO: test (0, 1, 59/60 seconds etc., test fallbacks, test future)
function formatTimestamp(timestamp_s) {
    const now = Math.floor((new Date()).getTime()/1000);
    const diff = Math.floor(now - timestamp_s); // timestamp_s can be a float (and now is)
    // ARCH: change from if statements to some for loop with decreasing granularity
    // also handle plurals betters
    if (diff < 0) {
        // future timestamps are tricky (clocks can get skewed) - also, even if we sent server time
        // as some guideline... we could still get a skew (not as bad as local time though)
        return (new Date(timestamp_s * 1000).toISOString());
    }
    if (diff < 60) {
        return `${diff} seconds ago`;
    }
    if (diff < 3600) {
        return `${Math.floor(diff/60)} minutes ago`;
    }
    if (diff < 3600*24) {
        return `${Math.floor(diff/3600)} hours ago`;
    }
    if (diff < 3600*24*30) {
        return `${Math.floor(diff/3600/24)} days ago`;
    }
    if (diff < 3600*24*365) {
        return `${Math.floor(diff/3600/24/30)} months ago`;
    }
    return `${Math.floor(diff/3600/24/30)} years ago`;
    // ARCH: should the fallback be years or ISO date?
    // return (new Date(timestamp * 1000).toISOString());
}

function formatDuration(ms, prefix) {
    if (ms < 1000) {
        return `${prefix} ${Math.round(ms)} ms`;
    }
    return `${prefix} ${(ms/1000).toFixed(2)} seconds`;
}

export { formatBytes, formatTimestamp, formatDuration };