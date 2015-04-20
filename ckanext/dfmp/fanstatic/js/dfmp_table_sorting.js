"use strict";

ckan.module('dfmp-table-sorting', function ($, _) {
  return {
    initialize: function(){
      $.proxyAll(this, /_on/);
      this.sorts = this.options.current.split(',');
      this.len = this.sorts.length;
      console.log(this.options);
      this.el.on('click', this._onClick);
    },

    _onClick: function(event){
      for( var i = 0; i < this.len; i++){
        var check = this.sorts[i].trim();
        if (check.indexOf(this.options.reverse) === 0){
          if (check.indexOf(' asc') !== -1){
            this.sorts[i] = this.sorts[i].replace(' asc', ' desc');
          }
          else if (check.indexOf(' desc') !== -1){
            this.sorts[i] = this.sorts[i].replace(' desc', ' asc');
          }
        }
      }
      window.location = window.location.pathname + '?sort=' + this.sorts.join(',');
    }
  }
})