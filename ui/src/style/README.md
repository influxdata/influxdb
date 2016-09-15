# Sass Organization

After spending some time with it, the general idea is that there are A LOT of opinions on how to structure sass files.
[This](https://blog.evernote.com/tech/2014/12/17/evernote-handles-sass-architecture/) seemed like a reasonable option.
Straightforward, lean, but with enough separation of concerns to make it worth the trouble in the first place.

* `/components` -- this is where most of our code will go.  Generally I think we'll have two types of components:
1) more generic components like `_button.scss` and `_form-input.scss`, and 2) partials that map to React components, e.g. `_Explorer.scss` and `_QueryEditor.scss`.
Most of the styles we'll end up writing will go in `/components`.
We've toyed around with the idea of putting React component styles directly next to the component itself, so that's something to try out potentially as well.

* `/base` -- global styles like resets/typography and things you'd depend on in the rest of the codebase like mixins, variables, colors, etc.

* `/layout` -- larger page-level styles

* `/themes` -- anything that might need to override styles, like a light/dark theme

Keep in mind this is a work in progress, and it will take some time trying things out to find a good balance.
