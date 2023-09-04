import React from "react";
import {FaFacebookF, FaInstagram, FaTwitter, FaYoutube} from "react-icons/fa";
import {Link} from "react-router-dom";
import './FooterLinks.scss'
import logoImg from '../../assets/shopito_logo.png'


const FooterLinks = () => {
    return(
        <>
            <section className="contact-section">
                <div className="container contact">
                    <div className="contact-icon">
                        <FaFacebookF className="i"/>
                        <FaTwitter className="i"/>
                        <FaInstagram className="i"/>
                        <FaYoutube className="i"/>
                    </div>
                    <h2>Chat Us</h2>
                    <Link to="/inquire" className="btn btn-dark">Make An Enquiry</Link>
                </div>
            </section>

            <section className="footer-section">
                <div className="container">
                   {/* <div className="footer-logo">
                        <img src={logoImg} alt="logo"/>
                    </div>*/}
                    <div className="footer-menu">
                        <p className="link-heading">
                            Features
                        </p>
                        <ul className="nav-ul footer-links">
                            <li className="nav-li">
                                <Link to="/about" className="nav-link">Links</Link>
                            </li>
                            <li className="nav-li">
                                <Link to="/about" className="nav-link">Branded Links</Link>
                            </li>
                            <li className="nav-li">
                                <Link to="/about" className="nav-link">Analytics</Link>
                            </li>
                            <li className="nav-li">
                                <Link to="/about" className="nav-link">Blog</Link>
                            </li>
                        </ul>
                    </div>


                </div>
            </section>
        </>
    )
}


export default FooterLinks;