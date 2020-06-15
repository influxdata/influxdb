export interface Point {
  measurement: string
  fields: PointFields
  tags: PointTags
  timestamp?: number
}

export interface PointTags {
  [key: string]: string
}

export interface PointFields {
  [key: string]: number | string
}

export interface Points {
  points: Array<Point>
  timestamp?: number
}

export const reportPoints = (points: Points) => {
  try {
    const url = '/api/v2/app-metrics'
    fetch(url, {
      method: 'POST',
      body: JSON.stringify(points),
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
    })
  } catch (e) {
    console.error(e)
    // don't want reporting errors to effect user experience
  }
}
