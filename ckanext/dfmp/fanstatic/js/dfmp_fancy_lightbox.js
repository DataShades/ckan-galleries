"use strict";

ckan.module('dfmp-fancy-lightbox', function ($, _) {
  return {
    options: {
      selector: 'empty-selector',
      title: ''
    },

    initialize: function(){
      if (!$.fancybox) return
      $.proxyAll(this, /_on/);

      $(this.options.selector).fancybox({
        'transitionIn'  : 'elastic',
        'transitionOut' : 'elastic',
        'speedIn'   : 300, 
        'speedOut'    : 100, 
        'overlayShow' : true,
        'padding' : 15,
        'overlayColor': "#000",
        'overlayOpacity': 0.2,
        'showCloseButton': false,
      });
      this.el.find('.preview-smallest-image').popover({
        html: true,
        content: '<div class="flag-preview-wrapper"><p>' + this.options.title + '</p><img src="' + this.options.thumbnail + '"></div>',
      })
      // this.el.on('click', this._onClick);
      this.el.on('mouseenter', this._onHoverStart);
      this.el.on('mouseleave', this._onHoverEnd);
    },

    _onHoverStart: function(event){
      this.el.find('.preview-smallest-image').popover('show');
    },

    _onHoverEnd: function(event){
      this.el.find('.preview-smallest-image').popover('hide');
    },


    _onClick: function(event){
      this.el.trigger('mouseleave');
    },

    _onClose: function(event){

    },

  }
})