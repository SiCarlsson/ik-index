import { useState } from "react";
import IonIcon from "@reacticons/ionicons";

import Logo from "../images/logoipsum.svg";

const Header = () => {
	let Links = [
		{ name: "Home", href: "/" },
		{ name: "Services", href: "/" },
		{ name: "About", href: "/" },
		{ name: "Contact", href: "/" },
	];

	let [isOpen, setisOpen] = useState(false);

	return (
		<div className="shadow-md w-full md:static fixed top-0 left-0">
			<div className="md:h-20 h-16 md:px-10 px-5 md:py-4 py-2 flex justify-center items-center bg-white">
				{/** Logo */}
				<div className="md:w-2/12 w-28 h-9 absolute md:left-10 left-5 flex justify-center">
					<img src={Logo} alt="Logotype" />
				</div>

				{/** Menu icon */}
				<div
					onClick={() => setisOpen(!isOpen)}
					className="absolute right-5 text-4xl md:hidden cursor-pointer flex justify-center"
				>
					{isOpen ? (
						<IonIcon name="close-outline" />
					) : (
						<IonIcon name="menu-outline" />
					)}
				</div>

				{/** Menu backdrop */}
				<div
					className={`md:hidden absolute w-full h-screen left-0 top-0 z-[-2] bg-black transition-opacity duration-500 ease-in
          ${isOpen ? "opacity-50" : "opacity-0"}`}
				></div>

				{/** Nav links */}
				<ul
					className={`md:flex md:justify-evenly md:static absolute md:py-0 py-2 md:pl-0
          bg-white md:w-6/12 w-full left-0 transition-all duration-500 md:z-auto z-[-1] ease-in
          ${isOpen ? "top-12" : "top-[-300px]"}`}
				>
					{Links.map((link) => (
						<li key={link.name} className="font-semibold md:my-0 my-7 md:ml-8 text-center">
							<a href={link.href}>{link.name}</a>
						</li>
					))}
				</ul>
			</div>
		</div>
	);
};

export default Header;