import React from 'react'
import { AiOutlineArrowLeft, AiOutlineArrowRight } from 'react-icons/ai'
import { useNavigate } from 'react-router-dom'


import { sliderData } from './SliderData'
import './Slider.scss'



const Slider = () => {
    const navigate = useNavigate()
    const [current, setCurrent] = React.useState(0);


    const nextSlide = () => {

    }
    const prevSlide = () => {

    }
    return (
        <div className='slider'>
            <AiOutlineArrowLeft className='arrow prev' onClick={prevSlide} />
            <AiOutlineArrowRight className='arrow next' onClick={nextSlide} />

            {
                sliderData.map((slide, index) => {
                    const { image, heading, desc } = slide
                    return (
                        <div className={index === current ? "slide current" : "slide"} key={index}>
                            {
                                index === current && (
                                    <>
                                        <img src={image} alt="slider" className='image' />
                                        <div className="content">
                                            <span className="span1"></span>
                                            <span className="span2"></span>
                                            <span className="span3"></span>
                                            <span className="span4"></span>
                                            <h2>{heading}</h2>
                                            <p>{desc}</p>
                                            <hr />
                                            <button className='--btn --btn-primary'
                                                onClick={() => navigate('/shop')}
                                            >Shop Now</button>
                                        </div>
                                    </>
                                )
                            }
                        </div>
                    )
                })
            }
        </div>
    )
}

export default Slider
