import React from 'react';

import FlashMessage from 'shared/components/FlashMessage';

export default function FlashMessages(ComposedComponent) {
  return React.createClass({
    getInitialState() {
      return {messages: []};
    },

    render() {
      const {messages} = this.state;
      return (
        <div>
          <div className="flash-messages">
            {messages.map((m, i) => {
              return <FlashMessage key={i} message={m} />;
            })}
          </div>
          <ComposedComponent
            {...this.props}
            addFlashMessage={this.handleNewFlashMessage}
          />
        </div>
      );
    },

    handleNewFlashMessage(message) {
      this.setState({
        messages: this.state.messages.concat(message),
      });
    },
  });
}
