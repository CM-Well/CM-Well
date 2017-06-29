$(document).ready(function () {
    var md = $('body').text(); // assuming browser wraps root content (excluding <head>) with <body>..</body>

    // temp hack to fix requests.md, dc*.md. this line can be removed after next upgrade.
    md=md.split('\n').map(function(line){return (line.trim().indexOf('|**')===0)?line.trim():line;}).join('\n');

    var converter = new Markdown.Converter();
    Markdown.Extra.init(converter);
    var formatted = converter.makeHtml(md);

    // javascript are already loaded and running.
    // other HEAD elements must be re-written, otherwise stylesheets won't be available.
    // this is because the head element was inside the implicit body...
    // the title was set, but if we rewrite head we need to preserve it.
    $('html').html('<head><title>'+document.title+'</title><link rel="stylesheet" href="/meta/sys/wb/js/highlight/styl'+
    'es/aiaas.css"/><link rel="stylesheet" href="/meta/sys/wb/css/cmwmdviewer.css"/></head><body><div id="cont"><div i'+
    'd="rendered"></div></div></body>');

    $('#rendered')
        .html(formatted)
        .find('code').each(function() { hljs.highlightBlock(this); });
});
