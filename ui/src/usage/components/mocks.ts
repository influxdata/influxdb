import {getOrgsBilling as gob} from 'src/client'

type GetOrgsBillingResult = ReturnType<typeof gob>

export const getOrgsBilling = new Promise(resolve =>
  resolve({
    status: 200,
    headers: null,
    data: {
      startDate: '2020-05-02',
      writeBytes: 248090,
      queryDurationSec: 23.49,
      storageGBHours: 52.23,
    },
  })
) as GetOrgsBillingResult
