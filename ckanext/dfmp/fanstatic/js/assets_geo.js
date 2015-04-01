var map;
    function initialize() {
        jQuery.ajax({
            dataType: 'json',
            url: '/api/3/action/get_last_geo_asset'
        }).done(function (response) {
            var asset = JSON.parse(response.result.data_dict);
            console.log(asset);

            var center = {
                    lat: -35.31397979,
                    lng: 149.12978252799996
            };

            if (asset.spatial.type == 'Polygon') {
                var bounds = new google.maps.LatLngBounds();

                // The Bermuda Triangle
                var polygonCoords = [];

                jQuery.each(asset.spatial.coordinates[0], function (key, val) {
                    polygonCoords.push(new google.maps.LatLng(val[1], val[0]));
                });

                for (var i = 0; i < polygonCoords.length; i++) {
                    bounds.extend(polygonCoords[i]);
                }

                center = {
                    lat: bounds.getCenter().lat(),
                    lng: bounds.getCenter().lng()
                };
            }
            else {
                center = {
                    lat: asset.spatial.coordinates[1],
                    lng: asset.spatial.coordinates[0]
                };
            }

            var mapOptions = {
                zoom: 10,
                center: new google.maps.LatLng(center.lat, center.lng)
            };

            map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);

            var image = new google.maps.InfoWindow({
                position: new google.maps.LatLng(center.lat, center.lng),
                map: map,
                content: '<div id="latest_asset_infowindow">' +
                            '<p>' + asset.name + '</p>' +
                            '<img src="' + asset.url + '"/>' +
                            '<p>Posted ' + new Date(asset.metadata_created).toTwitterRelativeTime() +  ' ago.</p>' +
                        '</div>',
                maxWidth: 180,
                disableAutoPan: false
            });
        });
    }
    google.maps.event.addDomListener(window, 'load', initialize);