// Libraries
import React, {FunctionComponent} from 'react'
import classnames from 'classnames'

interface Props {
  animate: boolean
}

const DashboardingGraphic: FunctionComponent<Props> = ({animate}) => {
  const className = classnames('getting-started--image dashboarding-graphic', {
    'getting-started--image__animating': animate,
  })
  return (
    <div className={className}>
      <svg
        version="1.1"
        x="0px"
        y="0px"
        width="322px"
        height="225px"
        viewBox="0 0 322 225"
      >
        <g>
          <path
            id="Background"
            className="dashboarding-graphic--bg"
            d="M291,30.5H31c-2.2,0-4,1.8-4,4v156c0,2.2,1.8,4,4,4h260c2.2,0,4-1.8,4-4v-156
  C295,32.3,293.2,30.5,291,30.5z"
          />
          <g id="Cells">
            <path
              className="dashboarding-graphic--cell"
              d="M162,41.5c0-1.7,1.3-3,3-3h55.5c1.7,0,3,1.3,3,3v23c0,1.7-1.3,3-3,3H165c-1.7,0-3-1.3-3-3V41.5z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M98.5,41.4c0.1-1.6,1.4-2.9,3-2.9H157c1.7,0,3,1.3,3,3v23c0,1.7-1.3,3-3,3h-55.5c-1.6,0-2.9-1.3-3-2.9
    c0,0,0-0.1,0-0.1v-23C98.5,41.5,98.5,41.4,98.5,41.4z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M35,41.5c0-1.7,1.3-3,3-3h55.5c1.6,0,2.9,1.3,3,2.9c0,0,0,0.1,0,0.1v23c0,0,0,0.1,0,0.1
    c-0.1,1.6-1.4,2.9-3,2.9H38c-1.7,0-3-1.3-3-3V41.5z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M35,72.5c0-1.7,1.3-3,3-3h55.5h8H157c1.7,0,3,1.3,3,3V132c0,1.7-1.3,3-3,3h-33h-8H38c-1.7,0-3-1.3-3-3V72.5z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M119,183.6c-0.1,1.6-1.4,2.9-3,2.9H38c-1.7,0-3-1.3-3-3V140c0-1.7,1.3-3,3-3h78c1.6,0,2.9,1.3,3,2.9
    c0,0,0,0.1,0,0.1v43.5C119,183.5,119,183.6,119,183.6z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M201,183.6c-0.1,1.6-1.4,2.9-3,2.9h-74c-1.6,0-2.9-1.3-3-2.9c0,0,0-0.1,0-0.1V140c0,0,0-0.1,0-0.1
    c0.1-1.6,1.4-2.9,3-2.9h33h8h33c1.6,0,2.9,1.3,3,2.9c0,0,0,0.1,0,0.1v43.5C201,183.5,201,183.6,201,183.6z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M287,183.5c0,1.7-1.3,3-3,3h-78c-1.6,0-2.9-1.3-3-2.9c0,0,0-0.1,0-0.1V140c0,0,0-0.1,0-0.1
    c0.1-1.6,1.4-2.9,3-2.9h78c1.7,0,3,1.3,3,3V183.5z"
            />
            <path
              className="dashboarding-graphic--cell"
              d="M287,64.5c0,1.7-1.3,3-3,3h-55.5c-1.7,0-3-1.3-3-3v-23c0-1.7,1.3-3,3-3H284c1.7,0,3,1.3,3,3V64.5z"
            />
            <path
              id="Cell"
              className="dashboarding-graphic--cell"
              d="M287,132c0,1.7-1.3,3-3,3h-78h-8h-33c-1.7,0-3-1.3-3-3V72.5c0-1.7,1.3-3,3-3h0h55.5h8H284
    c1.7,0,3,1.3,3,3V132z"
            />
          </g>
          <g id="Single_Stat_4392">
            <path
              className="dashboarding-graphic--single-stat single-stat-a"
              d="M57.8,54.1H59V55h-1.2v2h-1.1v-2h-3.9v-0.6l3.8-5.9h1.2V54.1z M54.1,54.1h2.7V50l-0.1,0.2L54.1,54.1z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-a"
              d="M61.5,52.2h0.8c0.5,0,0.9-0.1,1.2-0.4s0.4-0.6,0.4-1.1c0-1-0.5-1.5-1.5-1.5c-0.5,0-0.8,0.1-1.1,0.4
    S61,50.3,61,50.7h-1.1c0-0.7,0.2-1.2,0.7-1.7s1.1-0.7,1.9-0.7c0.8,0,1.4,0.2,1.9,0.6s0.7,1,0.7,1.8c0,0.4-0.1,0.7-0.4,1.1
    s-0.6,0.6-1,0.8c0.5,0.1,0.8,0.4,1.1,0.7s0.4,0.8,0.4,1.3c0,0.8-0.2,1.4-0.8,1.8s-1.2,0.7-2,0.7s-1.5-0.2-2-0.7s-0.8-1-0.8-1.7
    h1.1c0,0.4,0.1,0.8,0.4,1.1s0.7,0.4,1.2,0.4c0.5,0,0.9-0.1,1.2-0.4s0.4-0.7,0.4-1.2c0-0.5-0.2-0.9-0.5-1.1s-0.7-0.4-1.3-0.4h-0.8
    V52.2z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-a"
              d="M70.9,53.2c-0.2,0.3-0.5,0.5-0.8,0.7s-0.7,0.2-1,0.2c-0.5,0-0.9-0.1-1.3-0.4s-0.6-0.6-0.8-1s-0.3-0.9-0.3-1.5
    c0-0.6,0.1-1.1,0.3-1.5s0.5-0.8,0.9-1s0.9-0.4,1.4-0.4c0.8,0,1.5,0.3,2,0.9s0.7,1.5,0.7,2.6v0.3c0,1.7-0.3,2.9-1,3.6
    S69.3,57,68,57h-0.2v-0.9H68c0.9,0,1.6-0.2,2.1-0.7S70.8,54.2,70.9,53.2z M69.2,53.2c0.4,0,0.7-0.1,1-0.3s0.5-0.5,0.7-0.8v-0.4
    c0-0.7-0.2-1.3-0.5-1.7s-0.7-0.7-1.2-0.7c-0.5,0-0.9,0.2-1.1,0.5s-0.4,0.8-0.4,1.4c0,0.6,0.1,1.1,0.4,1.4S68.7,53.2,69.2,53.2z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-a"
              d="M79,57h-5.6v-0.8l3-3.3c0.4-0.5,0.7-0.9,0.9-1.2s0.2-0.6,0.2-1c0-0.4-0.1-0.8-0.4-1.1s-0.6-0.4-1.1-0.4
    c-0.5,0-1,0.2-1.3,0.5s-0.4,0.7-0.4,1.3h-1.1c0-0.8,0.3-1.4,0.8-1.9s1.2-0.7,2-0.7c0.8,0,1.4,0.2,1.9,0.6s0.7,1,0.7,1.7
    c0,0.8-0.5,1.8-1.6,3l-2.3,2.5H79V57z"
            />
          </g>
          <g id="Line_Graph_A">
            <polyline
              className="dashboarding-graphic--line line-a"
              points="40,121.8 54.4,121.8 68.8,115.5 83.1,118.6 97.5,109.8 111.9,114.2 126.2,109.8 140.6,109.8 
    155,117.2 		"
            />
            <polyline
              className="dashboarding-graphic--line line-b"
              points="40,118.2 54.4,118.2 68.8,104.8 83.1,74.5 97.5,99.5 111.9,92 126.2,103.8 140.6,105.5 155,109.8 		
    "
            />
            <polyline
              className="dashboarding-graphic--axes"
              points="40,74.5 40,130 155,130 		"
            />
          </g>
          <g id="Single_Stat_99.5">
            <path
              className="dashboarding-graphic--single-stat single-stat-b"
              d="M245.1,53.2c-0.2,0.3-0.5,0.5-0.8,0.7s-0.7,0.2-1,0.2c-0.5,0-0.9-0.1-1.3-0.4s-0.6-0.6-0.8-1
    s-0.3-0.9-0.3-1.5c0-0.6,0.1-1.1,0.3-1.5s0.5-0.8,0.9-1s0.9-0.4,1.4-0.4c0.8,0,1.5,0.3,2,0.9s0.7,1.5,0.7,2.6v0.3
    c0,1.7-0.3,2.9-1,3.6s-1.6,1.2-3,1.2H242v-0.9h0.2c0.9,0,1.6-0.2,2.1-0.7S245,54.2,245.1,53.2z M243.4,53.2c0.4,0,0.7-0.1,1-0.3
    s0.5-0.5,0.7-0.8v-0.4c0-0.7-0.2-1.3-0.5-1.7s-0.7-0.7-1.2-0.7c-0.5,0-0.9,0.2-1.1,0.5s-0.4,0.8-0.4,1.4c0,0.6,0.1,1.1,0.4,1.4
    S242.9,53.2,243.4,53.2z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-b"
              d="M251.8,53.2c-0.2,0.3-0.5,0.5-0.8,0.7s-0.7,0.2-1,0.2c-0.5,0-0.9-0.1-1.3-0.4s-0.6-0.6-0.8-1
    s-0.3-0.9-0.3-1.5c0-0.6,0.1-1.1,0.3-1.5s0.5-0.8,0.9-1s0.9-0.4,1.4-0.4c0.8,0,1.5,0.3,2,0.9s0.7,1.5,0.7,2.6v0.3
    c0,1.7-0.3,2.9-1,3.6s-1.6,1.2-3,1.2h-0.2v-0.9h0.2c0.9,0,1.6-0.2,2.1-0.7S251.7,54.2,251.8,53.2z M250.1,53.2
    c0.4,0,0.7-0.1,1-0.3s0.5-0.5,0.7-0.8v-0.4c0-0.7-0.2-1.3-0.5-1.7s-0.7-0.7-1.2-0.7c-0.5,0-0.9,0.2-1.1,0.5s-0.4,0.8-0.4,1.4
    c0,0.6,0.1,1.1,0.4,1.4S249.7,53.2,250.1,53.2z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-b"
              d="M254.5,56.4c0-0.2,0.1-0.3,0.2-0.5s0.3-0.2,0.5-0.2s0.4,0.1,0.5,0.2s0.2,0.3,0.2,0.5c0,0.2-0.1,0.3-0.2,0.5
    s-0.3,0.2-0.5,0.2s-0.4-0.1-0.5-0.2S254.5,56.6,254.5,56.4z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-b"
              d="M258,52.7l0.4-4.3h4.4v1h-3.5l-0.3,2.3c0.4-0.2,0.9-0.4,1.4-0.4c0.8,0,1.4,0.3,1.9,0.8s0.7,1.2,0.7,2.1
    c0,0.9-0.2,1.6-0.7,2.1s-1.1,0.8-2,0.8c-0.8,0-1.4-0.2-1.8-0.6s-0.7-1-0.8-1.7h1c0.1,0.5,0.2,0.9,0.5,1.1s0.7,0.4,1.1,0.4
    c0.5,0,0.9-0.2,1.2-0.5s0.4-0.8,0.4-1.4c0-0.6-0.2-1-0.5-1.4s-0.7-0.5-1.2-0.5c-0.5,0-0.8,0.1-1.1,0.3l-0.3,0.2L258,52.7z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-b"
              d="M264.2,50.1c0-0.5,0.2-0.9,0.5-1.3s0.7-0.5,1.3-0.5c0.5,0,0.9,0.2,1.3,0.5s0.5,0.8,0.5,1.3v0.4
    c0,0.5-0.2,0.9-0.5,1.3s-0.7,0.5-1.2,0.5c-0.5,0-0.9-0.2-1.3-0.5s-0.5-0.8-0.5-1.3V50.1z M265,50.6c0,0.3,0.1,0.6,0.3,0.8
    s0.4,0.3,0.7,0.3c0.3,0,0.5-0.1,0.7-0.3s0.3-0.5,0.3-0.8v-0.4c0-0.3-0.1-0.6-0.3-0.8c-0.2-0.2-0.4-0.3-0.7-0.3s-0.5,0.1-0.7,0.3
    c-0.2,0.2-0.3,0.5-0.3,0.8V50.6z M266.2,56.4l-0.6-0.4l4.2-6.7l0.6,0.4L266.2,56.4z M268.3,54.9c0-0.5,0.2-0.9,0.5-1.3
    s0.7-0.5,1.3-0.5s0.9,0.2,1.3,0.5s0.5,0.8,0.5,1.3v0.4c0,0.5-0.2,0.9-0.5,1.3s-0.7,0.5-1.3,0.5s-0.9-0.2-1.3-0.5s-0.5-0.8-0.5-1.3
    V54.9z M269.1,55.4c0,0.3,0.1,0.6,0.3,0.8s0.4,0.3,0.7,0.3c0.3,0,0.5-0.1,0.7-0.3s0.3-0.5,0.3-0.8v-0.4c0-0.3-0.1-0.6-0.3-0.8
    c-0.2-0.2-0.4-0.3-0.7-0.3c-0.3,0-0.5,0.1-0.7,0.3c-0.2,0.2-0.3,0.5-0.3,0.8V55.4z"
            />
          </g>
          <g id="Table">
            <rect
              x="167"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="115"
              height="55.5"
            />
            <rect
              x="167"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="23"
              height="55.5"
            />
            <rect
              x="190"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="23"
              height="55.5"
            />
            <rect
              x="213"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="23"
              height="55.5"
            />
            <rect
              x="236"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="23"
              height="55.5"
            />
            <rect
              x="259"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="23"
              height="55.5"
            />
            <rect
              x="167"
              y="74.5"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              x="167"
              y="82.4"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              x="167"
              y="90.4"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              x="167"
              y="98.3"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              x="167"
              y="106.2"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              x="167"
              y="114.1"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              x="167"
              y="122.1"
              className="dashboarding-graphic--axes"
              width="115"
              height="7.9"
            />
            <rect
              id="Highlight_Blue"
              x="190"
              y="82.4"
              className="dashboarding-graphic--axes table-a"
              width="23"
              height="7.9"
            />
            <rect
              id="Highlight_Purple"
              x="236"
              y="106.2"
              className="dashboarding-graphic--axes table-b"
              width="23"
              height="7.9"
            />
          </g>
          <g id="Line_Graph_B">
            <polyline
              className="dashboarding-graphic--line line-c"
              points="39.5,167.8 48.8,167.8 58.1,157 67.4,162.4 76.8,147 86.1,154.8 95.4,147 104.7,147 114,160 		"
            />
            <polyline
              className="dashboarding-graphic--axes"
              points="39.5,142 39.5,181.5 114,181.5 		"
            />
          </g>
          <g id="Bar_Chart_1_">
            <polyline
              className="dashboarding-graphic--axes"
              points="126,142 126,181.5 196,181.5 		"
            />
            <g id="Bar_Chart">
              <rect
                x="130"
                y="166.3"
                className="dashboarding-graphic--bar bar-a"
                width="6.3"
                height="15.2"
                transform="scale(1,-1) translate(0,-347.2)"
              />
              <rect
                x="139.1"
                y="149.3"
                className="dashboarding-graphic--bar bar-b"
                width="6.3"
                height="32.2"
                transform="scale(1,-1) translate(0,-330)"
              />
              <rect
                x="148.3"
                y="155.7"
                className="dashboarding-graphic--bar bar-c"
                width="6.3"
                height="25.8"
                transform="scale(1,-1) translate(0,-336.5)"
              />
              <rect
                x="157.4"
                y="177.5"
                className="dashboarding-graphic--bar bar-d"
                width="6.3"
                height="4"
                transform="scale(1,-1) translate(0,-358.5)"
              />
              <rect
                x="166.5"
                y="179.5"
                className="dashboarding-graphic--bar bar-e"
                width="6.3"
                height="2"
                transform="scale(1,-1) translate(0,-360.5)"
              />
              <rect
                x="175.6"
                y="173.9"
                className="dashboarding-graphic--bar bar-f"
                width="6.3"
                height="7.6"
                transform="scale(1,-1) translate(0,-355)"
              />
              <rect
                x="184.7"
                y="177.5"
                className="dashboarding-graphic--bar bar-g"
                width="6.3"
                height="4"
                transform="scale(1,-1) translate(0,-358.5)"
              />
            </g>
          </g>
          <g id="Line_Graph_C">
            <polyline
              className="dashboarding-graphic--line line-d"
              points="207.7,161 217.1,173.9 226.4,176.2 235.7,163.7 245,167.8 254.3,152.9 264.1,145 272.9,148 
282.3,148 "
            />
            <polyline
              className="dashboarding-graphic--axes"
              points="207.7,142 207.7,181.5 282.3,181.5 		"
            />
          </g>
          <g id="Single_Stat_4">
            <path
              className="dashboarding-graphic--single-stat single-stat-c"
              d="M194.7,53.9h1.2v0.9h-1.2v2h-1.1v-2h-3.9v-0.6l3.8-5.9h1.2V53.9z M190.9,53.9h2.7v-4.2l-0.1,0.2L190.9,53.9z
    "
            />
          </g>
          <g id="Single_Stat_ABC">
            <path
              className="dashboarding-graphic--single-stat single-stat-d"
              d="M123.4,54.8h-3.6L119,57h-1.2l3.3-8.5h1l3.3,8.5h-1.2L123.4,54.8z M120.2,53.8h2.9l-1.5-4L120.2,53.8z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-d"
              d="M126.5,57v-8.5h2.8c0.9,0,1.6,0.2,2.1,0.6s0.7,0.9,0.7,1.7c0,0.4-0.1,0.8-0.3,1.1s-0.5,0.5-0.9,0.7
    c0.5,0.1,0.8,0.4,1.1,0.7s0.4,0.8,0.4,1.3c0,0.8-0.2,1.4-0.7,1.8s-1.2,0.7-2.1,0.7H126.5z M127.6,52.1h1.7c0.5,0,0.9-0.1,1.2-0.4
    c0.3-0.2,0.4-0.6,0.4-1c0-0.5-0.1-0.8-0.4-1c-0.3-0.2-0.7-0.3-1.2-0.3h-1.7V52.1z M127.6,53v3.1h1.9c0.5,0,0.9-0.1,1.2-0.4
    s0.5-0.6,0.5-1.1c0-1-0.6-1.5-1.7-1.5H127.6z"
            />
            <path
              className="dashboarding-graphic--single-stat single-stat-d"
              d="M140.3,54.3c-0.1,0.9-0.4,1.6-1,2.1s-1.3,0.7-2.2,0.7c-1,0-1.8-0.4-2.4-1.1s-0.9-1.7-0.9-2.9v-0.8
    c0-0.8,0.1-1.5,0.4-2.1s0.7-1.1,1.2-1.4s1.1-0.5,1.8-0.5c0.9,0,1.6,0.3,2.2,0.8s0.9,1.2,1,2.1h-1.1c-0.1-0.7-0.3-1.2-0.6-1.5
    s-0.8-0.5-1.4-0.5c-0.7,0-1.3,0.3-1.7,0.8c-0.4,0.5-0.6,1.3-0.6,2.3v0.8c0,0.9,0.2,1.7,0.6,2.2s0.9,0.8,1.6,0.8
    c0.6,0,1.1-0.1,1.4-0.4s0.6-0.8,0.7-1.5H140.3z"
            />
          </g>
        </g>
      </svg>
    </div>
  )
}

export default DashboardingGraphic
