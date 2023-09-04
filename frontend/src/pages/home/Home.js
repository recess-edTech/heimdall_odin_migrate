/* eslint-disable no-unused-vars */

import Slider from '../../components/slider/Slider.js'


import React from 'react'

import './Home.scss'
import HomeInfo from "./HomeInfo.js";
import CarouselItem from "../../components/carousel/CarouselItem.js";
import {productData} from "../../components/carousel/Data.js";
import ProductCarousel from "../../components/carousel/Carousel.js";


const PageHeading = ({ heading, buttonText }) => {
    return (
        <>
            <div className="--flex-between">
                <h2 className="--fw-thin">{heading}</h2>
                <button className="--btn">{buttonText}</button>
            </div>
            <div className="--hr">

            </div>
        </>
    );
}
const Home = () => {
    const items = productData.map((item) => {
        return (
            <div key={item.id}>
                <CarouselItem
                    key={item.id}
                    url={item.imageurl}
                    name={item.name}
                    price={item.price}
                    description={item.description}
                />
            </div>
        );
    });
    return (
        <div>
            <Slider/>
            <section>
                <div className="container">
                    <HomeInfo/>
                    <PageHeading heading={"Amazing Products"} buttonText={"View All"} />
                    <ProductCarousel products={items} />
                </div>
            </section>
        </div>
    )};

export default Home
