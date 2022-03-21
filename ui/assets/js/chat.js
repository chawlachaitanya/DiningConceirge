var checkout = {};

$(document).ready(function () {
    var $messages = $('.messages-content'),
        d, h, m,
        i = 0;
    
    var current_session_id = null;

    function callPrevBookingApi(sessionId) {
        // params, body, additionalParams
        const url = "https://ln2dhmjkl4.execute-api.us-east-1.amazonaws.com/v1/suggestion?sessionId=" + sessionId;
        return fetch(url);
    }

    $(window).load(function () {
        $messages.mCustomScrollbar();
        
        let previous_session_id=window.localStorage.getItem('sessionId');
        if (previous_session_id){
            console.log(previous_session_id)
            callPrevBookingApi(previous_session_id)
            .then((response)=>{
                console.log(response)
                response.json()
                .then((resp)=>
                    {
                        console.log(resp)
                        let email=resp['recommendation']
                        if (email){
                            insertResponseMessage(email)
                        }

                    }
                )
                
                
            })
            .catch((error)=>{
                console.log(error)
            })
            .finally(()=>{
                insertResponseMessage('Hi there, I\'m your personal Concierge. How can I help?');
            })
        }
        else{
            insertResponseMessage('Hi there, I\'m your personal Concierge. How can I help?');
        }
        
    });

    function updateScrollbar() {
        $messages.mCustomScrollbar("update").mCustomScrollbar('scrollTo', 'bottom', {
            scrollInertia: 10,
            timeout: 0
        });
    }

    function setDate() {
        d = new Date()
        if (m != d.getMinutes()) {
            m = d.getMinutes();
            $('<div class="timestamp">' + d.getHours() + ':' + m + '</div>').appendTo($('.message:last'));
        }
    }

    function callChatbotApi(message) {
        // params, body, additionalParams
        let body = {
            messages: [{
                type: 'unstructured',
                unstructured: {
                    text: message
                }
            }]
        }
        if (current_session_id) {
            body.sessionId = current_session_id
        }
        return sdk.chatbotPost({}, body, {});
    }

    function insertMessage() {
        msg = $('.message-input').val();
        if ($.trim(msg) == '') {
            return false;
        }
        $('<div class="message message-personal">' + msg + '</div>').appendTo($('.mCSB_container')).addClass('new');
        setDate();
        $('.message-input').val(null);
        updateScrollbar();

        
        callChatbotApi(msg)
            .then((response) => {
                console.log(response);
                var data = response.data;
                localStorage.setItem('sessionId', data.sessionId)
                current_session_id=data.sessionId

                if (data.messages && data.messages.length > 0) {
                    console.log('received ' + data.messages.length + ' messages');

                    var messages = data.messages;

                    for (var message of messages) {
                        if (message.type === 'unstructured') {
                            insertResponseMessage(message.unstructured.text);
                        } else if (message.type === 'structured' && message.structured.type === 'product') {
                            var html = '';

                            insertResponseMessage(message.structured.text);

                            setTimeout(function () {
                                html = '<img src="' + message.structured.payload.imageUrl + '" witdth="200" height="240" class="thumbnail" /><b>' +
                                    message.structured.payload.name + '<br>$' +
                                    message.structured.payload.price +
                                    '</b><br><a href="#" onclick="' + message.structured.payload.clickAction + '()">' +
                                    message.structured.payload.buttonLabel + '</a>';
                                insertResponseMessage(html);
                            }, 1100);
                        } else {
                            console.log('not implemented');
                        }
                    }
                } else {
                    insertResponseMessage('Oops, something went wrong. Please try again.');
                }
            })
            .catch((error) => {
                console.log('an error occurred', error);
                insertResponseMessage('Oops, something went wrong. Please try again.');
            });
    }

    $('.message-submit').click(function () {
        insertMessage();
    });

    $(window).on('keydown', function (e) {
        if (e.which == 13) {
            insertMessage();
            return false;
        }
    })

    function insertResponseMessage(content) {
        $('<div class="message loading new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure><span></span></div>').appendTo($('.mCSB_container'));
        updateScrollbar();

        setTimeout(function () {
            $('.message.loading').remove();
            $('<div class="message new"><figure class="avatar"><img src="https://media.tenor.com/images/4c347ea7198af12fd0a66790515f958f/tenor.gif" /></figure>' + content + '</div>').appendTo($('.mCSB_container')).addClass('new');
            setDate();
            updateScrollbar();
            i++;
        }, 500);
    }

});
