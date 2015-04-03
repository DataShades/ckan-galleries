"use strict";

ckan.module('dfmp-removable-asset', function ($, _) {
  return {
    initialize: function(){
      $.proxyAll(this, /_on/);
      this.el.on('contextmenu', this._onContext);
      this.el.on('blur', this._onBlur);
    },

    _onContext: function(e){
      e.preventDefault();
      

    }
  }
});