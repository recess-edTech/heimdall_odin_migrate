import React from 'react'
import { AiOutlineArrowLeft, AiOutlineArrowRight } from 'react-icons/ai'
import { useNavigate } from 'react-router-dom'


import { sliderData } from './SliderData'
import './Slider.scss'


const Slider = () => {
    const navigate = useNavigate()
    const [current, setCurrent] = React.useState(0);
    const length = sliderData.length;
    const autoScroll = true;
    let sliderInterval;
    const intervalTime = 3000;

    const nextSlide = () => {
    setCurrent(current === length -1 ? 0 : current + 1)
    }
    const prevSlide = () => {
        setCurrent( current === 0 ? length - 1 : current - 1)
    }
    React.useEffect(()=>{
        setCurrent(0)
    }, [])
    React.useEffect(()=>{
        if(autoScroll){
            sliderInterval = setInterval(()=>{
                setCurrent(current === length -1 ? 0 : current + 1)
            }, intervalTime)
            return () => clearInterval(sliderInterval)
        }
    }, [current, autoScroll, intervalTime])
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
