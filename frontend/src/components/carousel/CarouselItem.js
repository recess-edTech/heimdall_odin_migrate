


import React from "react";
import {Link} from "react-router-dom";
import {shortenText} from "../../utils/index.js";
const CarouselItem = ({url, name, price, description})=>{
    return(
        <div className="carouselItem">
            <Link to="/product-details">
                <img src={url} alt={name} className="product--image"/>
                <p className="price">{`$${price} `}</p>
                <h4 className="name">{shortenText(name, 10)}</h4>
                <p className="description">{shortenText(description, 30)}</p>
            </Link>
            <button className="--btn --btn-primary --btn-block">Add to Cart</button>
        </div>
    )
}

export default CarouselItem;