/* eslint-disable no-unused-vars */

import Slider from '../../components/slider/Slider.js'


import React from 'react'
import './Home.scss'
import HomeInfo from "./HomeInfo.js";


const Home = () =>
    (
        <div>
            <Slider/>
            <section className="container">
                <HomeInfo/>
            </section>
        </div>
    )

export default Home
