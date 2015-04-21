// We define a function that takes one parameter named $.
(function ($) {
  $(document).ready(function () {
      $('#flickr_import_button').click(function (e) {

          var opts = {
              lines: 9, // The number of lines to draw
              length: 15, // The length of each line
              width: 6, // The line thickness
              radius: 13, // The radius of the inner circle
              corners: 1, // Corner roundness (0..1)
              rotate: 0, // The rotation offset
              direction: 1, // 1: clockwise, -1: counterclockwise
              color: '#000', // #rgb or #rrggbb or array of colors
              speed: 1, // Rounds per second
              trail: 60, // Afterglow percentage
              shadow: true, // Whether to render a shadow
              hwaccel: false, // Whether to use hardware acceleration
              className: 'spinner', // The CSS class to assign to the spinner
              zIndex: 2e9, // The z-index (defaults to 2000000000)
              top: '50%', // Top position relative to parent
              left: '50%' // Left position relative to parent
            };

          // URL pattern
          var pattern = /(https:\/\/)?(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?/;

          // URL provided by user
          var url = $('#flickr_pool_url').val();

          var site_url = $('#site-url-value').val();

          $('.alert-error').hide();
          $('.flash-messages').hide();

          if (!url) {
              flickr_notify ('error', 'Please provide a valid url.');
          }
          else if (url && !pattern.test(url)) {
              flickr_notify ('error', 'Provided url is not valid.');
          }
          else {
              $('.dataset-form').spin(opts);
              $('#flickr_import_button').attr('disabled', true);
              var request = $.ajax({
                  method : 'POST',
                  dataType : 'json',
                  url : site_url+ '/api/3/action/flickr_import_group_pool',
                  data : { url : url }
              })
              .done(function(response) {
                // enables spinner
                $('.dataset-form').spin(false);

                // disables import button
                $('#flickr_import_button').attr('disabled', false);

                // nwe need to notify user that the import has been started
                flickr_notify('success', response.result.text);

                // gets datastore id for further status updates
                var entity_id = response.result.datastore;

                if (!response.result.update) {
                    // updates status message
                    $('.flash-messages .status-update').html('Starting import...');
                }
                else {
                     $('.flash-messages .status-update').html('Starting pool update...');
                }

                // current status
                var current_status = 0;

                // udates stutus and notify user how many images have alreade been imported
                var update = setInterval(function () {
                  $.ajax({
                    method : 'POST',
                    dataType : 'json',
                    url : site_url+ '/api/3/action/task_status_show',
                    data : {
                      entity_id : entity_id,
                      key : 'celery_task_id',
                      task_type : 'flickr_import'
                    }
                  })
                  .done(function(response) {
                    if (response.result.state == current_status) {
                        return;
                    }

                    // updates current status
                    current_status = response.result.state;

                    // clears interval when import is done
                    if (response.result.state == 'done') {
                      clearInterval(update);
                      $('#flickr_import_button').attr('disabled', false);
                    }
                    // updates status message
                    $('.flash-messages .status-update').fadeOut().html(response.result.value).fadeIn();
                  });
                }, 5000)
              })
              .fail(function(response) {
                $('.dataset-form').spin(false);
                flickr_notify('error', $.parseJSON(response.responseText).error.message);
                $('#flickr_import_button').attr('disabled', false);
              });
          }

          e.stopPropagation();
          return false;
      });

      function flickr_notify (status, message) {
          if (status == 'error') {
            $('.alert-error p').html(message);
            $('.alert-error').show();
          }
          else if (status == 'success') {
            $('.flash-messages div.text').html(message);
            $('.flash-messages').show();
          }
      }
  });
}(jQuery));