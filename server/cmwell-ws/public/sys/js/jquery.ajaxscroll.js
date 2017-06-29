/**
 * AjaxScroll v0.1 (jQuery Plugins)
 *
 * @author Timmy Tin (ycTIN)
 * @license GPL
 * @version 0.1
 * @copyright Timmy Tin (ycTIN)
 * @website http://project.yctin.com/ajaxscroll
 *
 */
$.fn.ajaxScroll = function(opt){
    // settings
    opt = jQuery.extend({
        batchNum: 5,
        batchSize: 30,
        horizontal: false,
        
        batchTemplate: null,
        boxTemplate: null,
        
        batchClass: "batch",
        boxClass: "box",
        emptyBatchClass: "empty",
        scrollPaneClass: "scrollpane",
        
        lBound: "auto",
        uBound: "auto",
        eBound: "auto",
        maxOffset: 1000,
        scrollDelay: 600,
        endDelay: 100,
        updateBatch: null,
        updateEnd: null
    }, opt);
    
    return this.each(function(){
        var ele = this;
        var $me = jQuery(this);
        var $sp;
        var fnEnd, fnScroll;
        
        var offset = 0;
        var lsp = -1;
        
        //init. scrollpane
        _css();
        opt.boxTemplate = (opt.boxTemplate || "<span class='" + opt.boxClass + "'>&nbsp</span>");
        
        if (opt.horizontal) {
            opt.batchTemplate = (opt.batchTemplate || "<td></td>");
            $sp = jQuery("<table><tr></tr></table>").addClass(opt.scrollPaneClass);
            $me.append($sp);
            offset = batch($sp.find("tr"), offset, opt);
            
            _bz();
            _ab();
            
            fnEnd = hEnd;
            fnScroll = hScroll;
        } else {
            opt.batchTemplate = (opt.batchTemplate || "<span></span>");
            $sp = jQuery("<div></div>").addClass(opt.scrollPaneClass);
            $me.append($sp);
            offset = batch($sp, offset, opt);
            
            _bz();
            _ab();
            
            fnEnd = vEnd;
            fnScroll = vScroll;
        }
        
        //start monitoring
        setTimeout(monEnd, opt.endDelay);
        if (typeof opt.updateBatch == 'function') {
            setTimeout(monScroll, opt.scrollDelay);
        }
        
        //update scrollpane css
        function _css(){
            if (opt.horizontal) {
                $me.css({
                    "overflow-x": "auto",
                    "overflow-y": "hidden"
                });
            } else {
                $me.css({
                    "overflow-x": "hidden",
                    "overflow-y": "auto"
                });
            }
        }
        
        //auto calc bound
        function _ab(){
            var os, b;
            if (opt.horizontal) {
                os = $me.find('.batch:first').next().offset().left;
                b = ($me.width() / os + 1) * os;
            } else {
                os = $me.find('.batch:first').next().offset().top;
                b = ($me.height() / os + 1) * os;
            }
            if ("auto" == opt.uBound) {
                opt.uBound = b;
            }
            if ("auto" == opt.lBound) {
                opt.lBound = -b;
            }
            if ("auto" == opt.eBound) {
                opt.eBound = b * 2;
            }
            
        }
        
        //scroll to zero
        function _bz(){
            $me.scrollTop(0).scrollLeft(0);
        };
        
        //add new empty batch
        function batch($s, o, opt){
            var $b, i, rp = opt.batchNum;
            while (rp--) {
                $b = jQuery(opt.batchTemplate).attr({
                    offset: o,
                    len: opt.batchSize
                }).addClass(opt.batchClass + " " + opt.emptyBatchClass);
                
                i = opt.batchSize;
                while (i-- && opt.maxOffset > o++) {
                    $b.append(opt.boxTemplate);
                }
                $s.append($b);
            }
            return o;
        };
        function vScroll(){
            var so = $me.scrollTop();
            if (lsp != so) {
                lsp = so;
                var co = $me.offset().top;
                $sp.find('> .' + opt.emptyBatchClass).each(function(i, obj){
                    var $b = jQuery(obj);
                    var p = $b.position().top - co;
                    if (opt.lBound > p || p > opt.uBound) {
                        return;
                    }
                    opt.updateBatch($b.removeClass(opt.emptyBatchClass));
                });
            }
        };
        function hScroll(){
            var so = $me.scrollLeft();
            if (lsp != so) {
                lsp = so;
                var co = $me.offset().left;
                $sp.find('tr > .' + opt.emptyBatchClass).each(function(i, obj){
                    var $b = jQuery(obj);
                    var p = $b.position().left - co;
                    if (opt.lBound > p || p > opt.uBound) {
                        return;
                    }
                    opt.updateBatch($b.removeClass(opt.emptyBatchClass));
                });
            }
        };
        function vEnd(){
            if (ele.scrollTop > 0 && ele.scrollHeight - ele.scrollTop < opt.eBound) {
                offset = batch($sp, offset, opt);
                return 1;
            }
            return opt.endDelay;
        };
        function hEnd(){
            if (ele.scrollLeft > 0 && ele.scrollWidth - ele.scrollLeft < opt.eBound) {
                offset = batch($sp.find("tr:first"), offset, opt);
                return 1;
            }
            return opt.endDelay;
        };
        function monScroll(){
            fnScroll();
            setTimeout(monScroll, opt.scrollDelay);
        };
        function monEnd(){
            if (offset < opt.maxOffset) {
            	setTimeout(monEnd, fnEnd());
            }
        }
    });
};
