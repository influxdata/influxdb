import {PropTypes} from 'react';
const {number, string, arrayOf, bool, shape} = PropTypes;

const permissionShape = shape({
  name: string.isRequired,
  displayName: string.isRequired,
  description: string.isRequired,
  resources: arrayOf(string.isRequired).isRequired,
});

const roleShape = shape({
  name: string.isRequired,
  users: arrayOf(string.isRequired).isRequired,
  permissions: arrayOf(permissionShape).isRequired,
});

const clusterAccountShape = shape({
  name: string.isRequired,
  hash: string,
  permissions: arrayOf(permissionShape).isRequired,
  roles: arrayOf(roleShape).isRequired,
});

const lightClusterAccountShape = shape({
  id: number,
  name: string.isRequired,
  roles: string.isRequired,
});

const webUserShape = shape({
  id: number.isRequired,
  name: string.isRequired,
  email: string.isRequired,
  admin: bool.isRequired,
});

export {
  webUserShape,
  roleShape,
  permissionShape,
  clusterAccountShape,
  lightClusterAccountShape,
};
