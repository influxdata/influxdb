import React from 'react';
import FlashMessages from 'shared/components/FlashMessages';

export const HostPage = React.createClass({

  render() {
    return (
      <img src="http://i1215.photobucket.com/albums/cc506/Rockingbro/170422_dancing_banana.gif" />
    );
  },
});

export default FlashMessages(HostPage);
