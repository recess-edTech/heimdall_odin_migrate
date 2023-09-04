import React from "react";
import Carousel from "react-multi-carousel";
import "react-multi-carousel/lib/styles.css";
import {responsive} from "./Data.js";
import "./Carousel.scss";


const ProductCarousel = ({products}) => {
    return (
        <div>
            <Carousel
                swipeable={true}
                responsive={responsive}
                ssr={true} // means to render carousel on server-side.
                infinite={true}
                autoPlay={true}
                autoPlaySpeed={3000}
                keyBoardControl={true}
                customTransition="all 500ms ease"
                transitionDuration={1000}
            >
                {products}
            </Carousel>
        </div>
    );
}


export default ProductCarousel;